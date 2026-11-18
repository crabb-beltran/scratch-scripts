#!/usr/bin/env python3
"""Reinserta en <file> las llaves (líneas crudas) que existen en <ref> y faltan en el
archivo de trabajo, dentro del bloque de ambiente correcto, sin tocar líneas existentes.
Uso (desde la raíz del repo): python agregar_faltantes.py <file> <ref> [--dry-run]
Fuente: `git show <ref>:<file>`. Nunca imprime valores. Salida: 0 OK, 1 bloques solo en origen, 2 abortado."""
import argparse, re, subprocess, sys
import yaml

HDR = re.compile(r'^\s*-?\s*(co-[a-z-]+)\s*:\s*$')
KEY = re.compile(r'^(\s+)([A-Za-z][A-Za-z0-9_\-]*)\s*:')
PH = re.compile(r"(?<![{$])\{([A-Z][A-Z0-9_\-]*)\}(?!\})")


class Abort(Exception):
    pass


def split_lines(text):
    return re.findall(r'[^\n]*\n|[^\n]+', text)


def body(l):
    return l.rstrip('\r\n')


def indent(l):
    return len(l) - len(l.lstrip())


def is_cc(l):
    s = l.strip()
    return not s or s.startswith('#')


def parse_blocks(lines):
    """-> {bloque: {'hdr': idx, 'keys': [(llave, idx)]}}"""
    blocks = {}
    for h, l in enumerate(lines):
        m = HDR.match(body(l))
        if not m:
            continue
        if m.group(1) in blocks:
            raise Abort(f"bloque duplicado: {m.group(1)}")
        hi, keys = indent(l), []
        for i in range(h + 1, len(lines)):
            if is_cc(lines[i]):
                continue
            if HDR.match(body(lines[i])) or indent(lines[i]) <= hi:
                break
            k = KEY.match(body(lines[i]))
            if k:
                nxt = next((x for x in lines[i + 1:] if not is_cc(x)), None)
                if nxt is not None and indent(nxt) > len(k.group(1)):
                    raise Abort(f"valor multilínea no soportado en llave: {k.group(2)}")
                keys.append((k.group(2), i))
        blocks[m.group(1)] = {'hdr': h, 'keys': keys}
    return blocks


def dups(blocks):
    out = []
    for b, d in blocks.items():
        names = [k for k, _ in d['keys']]
        out += [f"{b}:{k}" for k in sorted({k for k in names if names.count(k) > 1})]
    return out


def yaml_blocks(text):
    try:
        envs = yaml.safe_load(PH.sub(r"${\1}", text))['environments']
        return {n: set(v or {}) for e in (envs if isinstance(envs, list) else [envs]) for n, v in e.items()}
    except Exception as e:
        raise Abort(f"YAML inválido ({type(e).__name__})")


def plan(sl, tl, sb, tb):
    """Calcula inserciones: {pos_en_target: [líneas fuente]} y reporte [(llave, ancla)]."""
    tkeys = {}
    for k, i in tb['keys']:
        tkeys.setdefault(k, i)
    sk, ins, rep, i = sb['keys'], {}, [], 0
    while i < len(sk):
        if sk[i][0] in tkeys:
            i += 1
            continue
        j = i
        while j + 1 < len(sk) and sk[j + 1][0] not in tkeys:
            j += 1
        anchor = sk[i - 1][0] if i else None
        a_src = sk[i - 1][1] if i else sb['hdr']
        pos = (tkeys[anchor] if i else tb['hdr']) + 1
        chunk = sl[a_src + 1:sk[j][1] + 1]
        if i and chunk and not chunk[0].strip():
            # grupo nuevo (viene con línea en blanco): va al final del grupo del ancla, no lo parte
            end = tb['keys'][-1][1] + 1
            while pos < end and tl[pos].strip():
                pos += 1
        ins.setdefault(pos, []).extend(chunk)
        rep += [(k, anchor) for k, _ in sk[i:j + 1]]
        i = j + 1
    return ins, rep


def apply(tl, ins, eol):
    out = []
    for idx in range(len(tl) + 1):
        if idx in ins:
            if out and not out[-1].endswith('\n'):
                out[-1] += eol
            out += [body(l) + eol for l in ins[idx]]
        if idx < len(tl):
            out.append(tl[idx])
    return out


def validate(src_text, tl, new, eol):
    sb, nb = parse_blocks(split_lines(src_text)), parse_blocks(new)
    if dups(nb):
        raise Abort("llaves duplicadas: " + ", ".join(dups(nb)))
    ys, yn = yaml_blocks(src_text), yaml_blocks(''.join(new))
    for b in sb:
        if b in nb and (not {k for k, _ in sb[b]['keys']} <= {k for k, _ in nb[b]['keys']}
                        or not ys.get(b, set()) <= yn.get(b, set())):
            raise Abort(f"faltan llaves en {b} tras insertar")
    it = iter(body(l) for l in new)
    if not all(any(body(o) == n for n in it) for o in tl):
        raise Abort("una línea preexistente cambió o se reordenó")
    ends = lambda ls: {l[len(body(l)):] for l in ls} - {''}
    if not ends(new) <= (ends(tl) | {eol}) or (len(ends(tl)) <= 1 and len(ends(new)) > 1):
        raise Abort("mezcla de finales de línea")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('file'), ap.add_argument('ref'), ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    try:
        r = subprocess.run(['git', 'show', f'{a.ref}:{a.file}'], capture_output=True)
        if r.returncode:
            raise Abort(f"git show falló para {a.ref}:{a.file}")
        src = r.stdout.decode('utf-8', 'surrogateescape')
        with open(a.file, 'rb') as f:
            tgt = f.read().decode('utf-8', 'surrogateescape')
        eol = '\r\n' if tgt.count('\r\n') * 2 > tgt.count('\n') else '\n'
        sl, tl = split_lines(src), split_lines(tgt)
        sb, tb = parse_blocks(sl), parse_blocks(tl)
        if dups(tb):
            raise Abort("llaves duplicadas en el archivo de trabajo: " + ", ".join(dups(tb)))
        ins, total = {}, 0
        for name, d in tb.items():
            if name in sb:
                i, rep = plan(sl, tl, sb[name], d)
                if rep:
                    print(f"{name}: {len(rep)} llaves agregadas")
                    for k, anc in rep:
                        print(f"  + {k}  (después de {anc})" if anc else f"  + {k}  (inicio del bloque)")
                ins.update(i)
                total += len(rep)
        solo = [b for b in sb if b not in tb]
        if solo:
            print("Bloques solo en origen (omitidos): " + ", ".join(solo))
        if total:
            new = apply(tl, ins, eol)
            validate(src, tl, new, eol)
            if not a.dry_run:
                with open(a.file, 'wb') as f:
                    f.write(''.join(new).encode('utf-8', 'surrogateescape'))
            else:
                print("(dry-run: no se escribió nada)")
        else:
            print("Sin llaves faltantes")
        print("RESULTADO: OK" if not solo else "RESULTADO: INCOMPLETO (bloques solo en origen)")
        return 1 if solo else 0
    except Abort as e:
        print(f"RESULTADO: ABORTADO - {e}")
        return 2


if __name__ == '__main__':
    sys.exit(main())
