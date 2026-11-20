"""Minimal anonymous Internet Computer (ICP) query client: CBOR envelope + Candid decoding, stdlib only.

Used for sites built on Caffeine / ICP whose pages load their data with an ANONYMOUS public canister query
(the same unsigned `query` call the site's own JavaScript agent makes; no tokens, no forged headers).
carwashbrokers.com: GET /env.json -> backend_canister_id; POST <host>/api/v2/canister/<id>/query listListings.
"""
import base64, json, struct, time, urllib.request

def _cbor_head(major, n):
    if n < 24: return bytes([major << 5 | n])
    if n < 256: return bytes([major << 5 | 24, n])
    if n < 65536: return bytes([major << 5 | 25]) + struct.pack('>H', n)
    if n < 2**32: return bytes([major << 5 | 26]) + struct.pack('>I', n)
    return bytes([major << 5 | 27]) + struct.pack('>Q', n)

def cbor_enc(o):
    if isinstance(o, bool): return b'\xf5' if o else b'\xf4'
    if isinstance(o, int): return _cbor_head(0, o)
    if isinstance(o, bytes): return _cbor_head(2, len(o)) + o
    if isinstance(o, str):
        b = o.encode(); return _cbor_head(3, len(b)) + b
    if isinstance(o, dict):
        return _cbor_head(5, len(o)) + b''.join(cbor_enc(k) + cbor_enc(v) for k, v in o.items())
    if isinstance(o, list):
        return _cbor_head(4, len(o)) + b''.join(cbor_enc(v) for v in o)
    raise TypeError(type(o))

def cbor_dec(b, i=0):
    ib = b[i]; mt, ai = ib >> 5, ib & 31; i += 1
    if ai < 24: n = ai
    elif ai == 24: n = b[i]; i += 1
    elif ai == 25: n = struct.unpack('>H', b[i:i+2])[0]; i += 2
    elif ai == 26: n = struct.unpack('>I', b[i:i+4])[0]; i += 4
    elif ai == 27: n = struct.unpack('>Q', b[i:i+8])[0]; i += 8
    elif ai == 31:
        if mt in (4, 5):
            out = [] if mt == 4 else {}
            while b[i] != 0xff:
                if mt == 4: v, i = cbor_dec(b, i); out.append(v)
                else:
                    k, i = cbor_dec(b, i); v, i = cbor_dec(b, i); out[k] = v
            return out, i + 1
        if mt in (2, 3):
            parts = b''
            while b[i] != 0xff:
                v, i = cbor_dec(b, i); parts += v if mt == 2 else v.encode()
            return (parts if mt == 2 else parts.decode()), i + 1
        raise ValueError('indefinite')
    else: raise ValueError('bad additional info')
    if mt == 0: return n, i
    if mt == 1: return -1 - n, i
    if mt == 2: return b[i:i+n], i + n
    if mt == 3: return b[i:i+n].decode(), i + n
    if mt == 4:
        out = []
        for _ in range(n): v, i = cbor_dec(b, i); out.append(v)
        return out, i
    if mt == 5:
        out = {}
        for _ in range(n):
            k, i = cbor_dec(b, i); v, i = cbor_dec(b, i); out[k] = v
        return out, i
    if mt == 6: return cbor_dec(b, i)          # tag: return the tagged value
    if mt == 7: return {20: False, 21: True, 22: None}.get(ai), i
    raise ValueError(mt)

def principal_bytes(text):
    s = text.replace('-', '').upper(); s += '=' * (-len(s) % 8)
    return base64.b32decode(s)[4:]

# ---- Candid decoding (subset) ----
def _hash(name):
    h = 0
    for c in name.encode(): h = (h * 223 + c) % 2**32
    return h

def _leb(b, i):
    r = s = 0
    while True:
        x = b[i]; i += 1; r |= (x & 127) << s; s += 7
        if not x & 128: return r, i

def _sleb(b, i):
    r = s = 0
    while True:
        x = b[i]; i += 1; r |= (x & 127) << s; s += 7
        if not x & 128:
            if x & 64: r -= 1 << s
            return r, i

def candid_decode(b, names=()):
    assert b[:4] == b'DIDL', b[:8]
    hashes = {_hash(n): n for n in names}
    i = 4; n, i = _leb(b, i); table = []
    for _ in range(n):
        op, i = _sleb(b, i)
        if op in (-18, -19):                       # opt / vec
            t, i = _sleb(b, i); table.append((op, t))
        elif op in (-20, -21):                     # record / variant
            m, i = _leb(b, i); fs = []
            for _ in range(m):
                h, i = _leb(b, i); t, i = _sleb(b, i); fs.append((h, t))
            table.append((op, fs))
        else: raise ValueError('type op %d' % op)
    na, i = _leb(b, i); types = []
    for _ in range(na): t, i = _sleb(b, i); types.append(t)
    def val(t, i):
        if t >= 0:
            op, d = table[t]
            if op == -18:
                f = b[i]; i += 1
                return (None, i) if f == 0 else val(d, i)
            if op == -19:
                m, i = _leb(b, i); out = []
                for _ in range(m): v, i = val(d, i); out.append(v)
                return out, i
            if op == -20:
                out = {}
                for h, ft in d: v, i = val(ft, i); out[hashes.get(h, h)] = v
                return out, i
            if op == -21:
                k, i = _leb(b, i); h, ft = d[k]; v, i = val(ft, i); return {hashes.get(h, h): v}, i
        if t == -1: return None, i
        if t == -2: return bool(b[i]), i + 1
        if t == -3: return _leb(b, i)
        if t == -4: return _sleb(b, i)
        if t == -15:
            n, i = _leb(b, i); return b[i:i+n].decode(), i + n
        raise ValueError('prim %d' % t)
    out = []
    for t in types: v, i = val(t, i); out.append(v)
    return out

def query(host, canister_text, method, arg):
    cid = principal_bytes(canister_text)
    env = {"content": {"request_type": "query", "canister_id": cid, "method_name": method, "arg": arg,
                       "sender": b"\x04", "ingress_expiry": int((time.time() + 240) * 1e9)}}
    body = b'\xd9\xd9\xf7' + cbor_enc(env)
    req = urllib.request.Request(f"{host}/api/v2/canister/{canister_text}/query", data=body,
                                 headers={"Content-Type": "application/cbor"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r: raw = r.read()
    d, _ = cbor_dec(raw)
    if d.get("status") != "replied": raise RuntimeError(d)
    return d["reply"]["arg"]

