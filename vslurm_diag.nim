## rustc-style diagnostics for user-submitted sbatch files and CLI usage.
## A diagnostic carries a span into the offending line, carets under the
## exact token, a plain-language `error`/`warning` summary, `note` context
## (optionally with its own span — multi-point blame), and an actionable
## `help` with a copy-pasteable fix. Diagnostics are collected, not
## printed, so a tool can report every problem in one pass and exit once.
## Also hosts the span-aware #SBATCH directive scanner: it mirrors
## vslurm_common.scanArgs/shellWords semantics but keeps token positions.

import os, strutils, terminal

type
  Severity* = enum sevWarning = "warning", sevError = "error"

  Span* = object
    ## Location of an offending token: 1-based line/column and the token's
    ## length in characters. line == 0 means "no location" (a bare CLI
    ## message with no script context to point into).
    line*: int
    col*: int
    len*: int

  Related* = tuple[path: string, span: Span, label: string]

  Diag* = object
    sev*: Severity
    tool*: string          ## "sbatch" / "srun" — prefixes the summary
    msg*: string           ## concise summary of the violation
    path*: string          ## script the span points into, if any
    span*: Span            ## underline target; span.line == 0 => no span
    spanLabel*: string     ## text drawn after the carets
    related*: seq[Related] ## extra blame points, e.g. where a block started
    notes*: seq[string]    ## passive context: why the parser arrived here
    helps*: seq[string]    ## proactive fixes: exact syntax to use
    fixes*: seq[string]    ## copy-pasteable replacement lines

proc noSpan*(): Span = Span(line: 0, col: 1, len: 0)

proc at*(line, col, len: int): Span = Span(line: line, col: col, len: len)

proc plain*(sev: Severity; tool, msg: string): Diag =
  Diag(sev: sev, tool: tool, msg: msg, span: noSpan())

proc with*(d: Diag; path: string; s: Span; label = ""): Diag =
  var r = d
  r.path = path
  r.span = s
  if label.len > 0: r.spanLabel = label
  r

proc note*(d: Diag; text: string): Diag =
  var r = d
  r.notes.add text
  r

proc relatedTo*(d: Diag; path: string; s: Span; label: string): Diag =
  var r = d
  r.related.add (path, s, label)
  r

proc help*(d: Diag; text: string; fix = ""): Diag =
  var r = d
  r.helps.add text
  if fix.len > 0: r.fixes.add fix
  r

# --- did-you-mean: Jaro-Winkler over a candidate set ------------------------

proc jaro(a, b: string): float =
  if a.len == 0 or b.len == 0: return 0.0
  let window = max(a.len, b.len) div 2 - 1
  var amatch = newSeq[bool](a.len)
  var bmatch = newSeq[bool](b.len)
  var matches = 0
  for i in 0 ..< a.len:
    let lo = max(0, i - window)
    let hi = min(b.len - 1, i + window)
    for j in lo .. hi:
      if not bmatch[j] and a[i] == b[j]:
        amatch[i] = true
        bmatch[j] = true
        inc matches
        break
  if matches == 0: return 0.0
  var t = 0
  var k = 0
  for i in 0 ..< a.len:
    if amatch[i]:
      while not bmatch[k]: inc k
      if a[i] != b[k]: inc t
      inc k
  let m = matches.float
  (m / a.len.float + m / b.len.float + (m - t.float / 2) / m) / 3

proc didYouMean*(typed: string; candidates: openArray[string]): string =
  ## The closest candidate, or "" when nothing is close enough. Winkler's
  ## common-prefix boost is what catches dropped-letter typos
  ## (`timout` -> `time` scores only 0.75 as plain Jaro).
  var best = ""
  var bestScore = 0.0
  let a = typed.toLowerAscii
  for c in candidates:
    let b = c.toLowerAscii
    var s = jaro(a, b)
    var p = 0
    while p < 4 and p < a.len and p < b.len and a[p] == b[p]: inc p
    s = s + p.float * 0.1 * (1 - s)
    if s > bestScore:
      bestScore = s
      best = c
  if bestScore >= 0.80 and best != typed: result = best else: result = ""

# --- rendering ---------------------------------------------------------------

func colored(s: string; code: int; on: bool): string =
  if on: "\x1b[" & $code & "m" & s & "\x1b[0m" else: s

proc expandTabs(src: string; upto: int): tuple[dsp: string, dcol: int] =
  ## Tab-expand a source line for display; upto is the 1-based raw column
  ## whose display column we need, so carets land under the right glyphs.
  result = (src.replace("\t", "        "), upto)
  var d = 1
  for i in 0 ..< src.len:
    if i + 1 == upto:
      result.dcol = d
      break
    if src[i] == '\t':
      d = ((d - 1) div 8 + 1) * 8 + 1
    else:
      inc d

proc readLine(path: string; n: int): string =
  var f: File
  if not f.open(path): return ""
  defer: f.close()
  var i = 0
  for line in f.lines:
    inc i
    if i == n: return line
  ""

proc underlineFix(fix: string): tuple[col, len: int] =
  ## Span of the first `--flag` token in a fix line, to point carets at
  ## the corrected spelling.
  let k = fix.find("--")
  if k < 0: return (0, 0)
  var e = k
  while e < fix.len and fix[e] in {'a'..'z', 'A'..'Z', '0'..'9', '-'}: inc e
  (k + 1, e - k)

proc spanBlock(path: string; span: Span; label: string; useColor: bool): string =
  let raw = readLine(path, span.line)
  if raw.len == 0: return ""
  let ex = expandTabs(raw, span.col)
  let w = ($span.line).len
  let pad = spaces(w)
  let col0 = max(ex.dcol - 1, 0)
  let hlen = max(1, min(span.len, max(ex.dsp.len - col0, 1)))
  result = pad & " --> " & path & ":" & $span.line & ":" & $span.col
  result.add "\n" & pad & "  |"
  result.add "\n" & $span.line & " | " & ex.dsp
  result.add "\n" & pad & "  | " & spaces(col0) & "^".repeat(hlen)
  if label.len > 0:
    result.add " " & label.colored(36, useColor)

proc render*(d: Diag): string =
  ## `sbatch: error: <msg>` header, then the spanned line with carets,
  ## notes (each with its own span), then helps and fixes — rustc's
  ## layered structure, kept compact.
  # NO_COLOR (https://no-color.org) and non-tty both disable color
  let useColor = isatty(stderr) and getEnv("NO_COLOR").len == 0 and
    getEnv("TERM") != "dumb"
  let tag = ($d.sev).colored(if d.sev == sevError: 31 else: 33, useColor)
  result = d.tool & ": " & tag & ": " & d.msg

  let w = if d.span.line > 0: ($d.span.line).len else: 1
  let pad = spaces(w)

  if d.span.line > 0 and d.path.len > 0:
    result.add "\n" & spanBlock(d.path, d.span, d.spanLabel, useColor)

  for n in d.notes:
    result.add "\nnote: " & n
  for r in d.related:
    result.add "\nnote: " & r.label
    result.add "\n" & spanBlock(r.path, r.span, "", useColor)
  for h in d.helps:
    result.add "\nhelp: " & h
  for fx in d.fixes:
    result.add "\n" & pad & "  |"
    if d.span.line > 0:
      result.add "\n" & $d.span.line & " | " & fx
      let u = underlineFix(fx)
      if u.len > 0:
        result.add "\n" & pad & "  | " & spaces(max(u.col - 1, 0)) &
          "^".repeat(u.len)
    else:
      result.add "\n" & pad & "  | " & fx

proc emit*(d: Diag) =
  stderr.writeLine(render(d))

proc drainPlain*(warned: var seq[string]) =
  ## Print already-formatted legacy messages collected by warn/warnOnce.
  ## Tools that keep span tracking render richer diagnostics instead and
  ## never call this; srun keeps it until it grows span tracking too.
  for m in warned:
    stderr.writeLine(m)
  warned.setLen(0)

# --- span-aware #SBATCH directive scanning ----------------------------------
# Mirrors vslurm_common shellWords + scanArgs (quotes, backslash escapes,
# unknown options never consuming the next token) while keeping each
# token's position on the directive line.

type
  SpannedTok* = object
    tok*: string
    col*: int     ## 1-based column of the token in the physical line
    rawLen*: int  ## extent of the token as written (quotes included)

  DirCall* = object
    opt*: string   ## long option name, or the bare char for unknown shorts
    val*: string
    raw*: string   ## the flag token exactly as written
    optSpan*: Span
    valSpan*: Span
    hasVal*: bool

proc spannedWords*(s: string; base = 0): seq[SpannedTok] =
  ## shellWords, but every word remembers where it came from. `base` is
  ## the 0-based offset of `s` inside the physical line.
  result = @[]
  var cur = ""
  var start = -1
  var i = 0
  template done =
    if start >= 0:
      result.add SpannedTok(tok: cur, col: base + start + 1,
        rawLen: i - start)
      cur = ""
      start = -1
  while i < s.len:
    let c = s[i]
    case c
    of ' ', '\t':
      done
    of '\'', '"':
      if start < 0: start = i
      let q = c
      inc i
      while i < s.len and s[i] != q:
        if q == '"' and s[i] == '\\' and i + 1 < s.len and s[i + 1] == '"':
          cur.add '"'
          inc i
        else:
          cur.add s[i]
        inc i
    of '\\':
      if start < 0: start = i
      if i + 1 < s.len:
        cur.add s[i + 1]
        inc i
      else:
        cur.add '\\'
    else:
      if start < 0: start = i
      cur.add c
    inc i
  done

proc scanDirective*(words: seq[SpannedTok]; line: int;
    shortMap: openArray[tuple[s: char, l: string]];
    valueOpts: openArray[string]): tuple[calls: seq[DirCall],
    positionals: seq[SpannedTok]] =
  ## scanArgs over spanned words: same option grammar as the CLI, but each
  ## call knows which characters on the directive line it came from.
  result.calls = @[]
  result.positionals = @[]
  var i = 0
  while i < words.len:
    let w = words[i]
    let t = w.tok
    if t == "--":
      for j in i + 1 ..< words.len: result.positionals.add words[j]
      break
    if t.startsWith("--") and t.len > 2:
      let body = t[2 .. ^1]
      let eq = body.find('=')
      if eq >= 0:
        let v = body[eq + 1 .. ^1]
        result.calls.add DirCall(opt: body[0 ..< eq], val: v, raw: t,
          optSpan: at(line, w.col, eq + 2),
          valSpan: at(line, w.col + eq + 3, max(v.len, 1)), hasVal: true)
      elif body in valueOpts and i + 1 < words.len:
        inc i
        result.calls.add DirCall(opt: body, val: words[i].tok, raw: t,
          optSpan: at(line, w.col, w.rawLen),
          valSpan: at(line, words[i].col, words[i].rawLen), hasVal: true)
      else:
        result.calls.add DirCall(opt: body, val: "", raw: t,
          optSpan: at(line, w.col, w.rawLen), valSpan: noSpan(), hasVal: false)
    elif t.len >= 2 and t[0] == '-':
      let c = t[1]
      var long = ""
      for m in shortMap:
        if m.s == c:
          long = m.l
          break
      if long == "":
        result.calls.add DirCall(opt: $c, val: "", raw: t,
          optSpan: at(line, w.col, w.rawLen), valSpan: noSpan(), hasVal: false)
      elif t.len > 2:
        result.calls.add DirCall(opt: long, val: t[2 .. ^1], raw: t,
          optSpan: at(line, w.col, 2),
          valSpan: at(line, w.col + 2, w.rawLen - 2), hasVal: true)
      elif long in valueOpts and i + 1 < words.len:
        inc i
        result.calls.add DirCall(opt: long, val: words[i].tok, raw: t,
          optSpan: at(line, w.col, w.rawLen),
          valSpan: at(line, words[i].col, words[i].rawLen), hasVal: true)
      else:
        result.calls.add DirCall(opt: long, val: "", raw: t,
          optSpan: at(line, w.col, w.rawLen), valSpan: noSpan(), hasVal: false)
    else:
      result.positionals.add w
    inc i
