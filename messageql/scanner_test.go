package messageql_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/messagedb/messagedb/messageql"
)

// Ensure the scanner can scan tokens correctly.
func TestScanner_Scan(t *testing.T) {
	var tests = []struct {
		s   string
		tok messageql.Token
		lit string
		pos messageql.Pos
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: messageql.EOF},
		{s: `#`, tok: messageql.ILLEGAL, lit: `#`},
		{s: ` `, tok: messageql.WS, lit: " "},
		{s: "\t", tok: messageql.WS, lit: "\t"},
		{s: "\n", tok: messageql.WS, lit: "\n"},
		{s: "\r", tok: messageql.WS, lit: "\n"},
		{s: "\r\n", tok: messageql.WS, lit: "\n"},
		{s: "\rX", tok: messageql.WS, lit: "\n"},
		{s: "\n\r", tok: messageql.WS, lit: "\n\n"},
		{s: " \n\t \r\n\t", tok: messageql.WS, lit: " \n\t \n\t"},
		{s: " foo", tok: messageql.WS, lit: " "},

		// Numeric operators
		{s: `+`, tok: messageql.ADD},
		{s: `-`, tok: messageql.SUB},
		{s: `*`, tok: messageql.MUL},
		{s: `/`, tok: messageql.DIV},

		// Logical operators
		{s: `AND`, tok: messageql.AND},
		{s: `and`, tok: messageql.AND},
		{s: `OR`, tok: messageql.OR},
		{s: `or`, tok: messageql.OR},

		{s: `=`, tok: messageql.EQ},
		{s: `<>`, tok: messageql.NEQ},
		{s: `! `, tok: messageql.ILLEGAL, lit: "!"},
		{s: `<`, tok: messageql.LT},
		{s: `<=`, tok: messageql.LTE},
		{s: `>`, tok: messageql.GT},
		{s: `>=`, tok: messageql.GTE},

		// Misc tokens
		{s: `(`, tok: messageql.LPAREN},
		{s: `)`, tok: messageql.RPAREN},
		{s: `,`, tok: messageql.COMMA},
		{s: `;`, tok: messageql.SEMICOLON},
		{s: `.`, tok: messageql.DOT},
		{s: `=~`, tok: messageql.EQREGEX},
		{s: `!~`, tok: messageql.NEQREGEX},

		// Identifiers
		{s: `foo`, tok: messageql.IDENT, lit: `foo`},
		{s: `_foo`, tok: messageql.IDENT, lit: `_foo`},
		{s: `Zx12_3U_-`, tok: messageql.IDENT, lit: `Zx12_3U_`},
		{s: `"foo"`, tok: messageql.IDENT, lit: `foo`},
		{s: `"foo\\bar"`, tok: messageql.IDENT, lit: `foo\bar`},
		{s: `"foo\bar"`, tok: messageql.BADESCAPE, lit: `\b`, pos: messageql.Pos{Line: 0, Char: 5}},
		{s: `"foo\"bar\""`, tok: messageql.IDENT, lit: `foo"bar"`},
		{s: `test"`, tok: messageql.BADSTRING, lit: "", pos: messageql.Pos{Line: 0, Char: 3}},
		{s: `"test`, tok: messageql.BADSTRING, lit: `test`},

		{s: `true`, tok: messageql.TRUE},
		{s: `false`, tok: messageql.FALSE},

		// Strings
		{s: `'testing 123!'`, tok: messageql.STRING, lit: `testing 123!`},
		{s: `'foo\nbar'`, tok: messageql.STRING, lit: "foo\nbar"},
		{s: `'foo\\bar'`, tok: messageql.STRING, lit: "foo\\bar"},
		{s: `'test`, tok: messageql.BADSTRING, lit: `test`},
		{s: "'test\nfoo", tok: messageql.BADSTRING, lit: `test`},
		{s: `'test\g'`, tok: messageql.BADESCAPE, lit: `\g`, pos: messageql.Pos{Line: 0, Char: 6}},

		// Numbers
		{s: `100`, tok: messageql.NUMBER, lit: `100`},
		{s: `100.23`, tok: messageql.NUMBER, lit: `100.23`},
		{s: `+100.23`, tok: messageql.NUMBER, lit: `+100.23`},
		{s: `-100.23`, tok: messageql.NUMBER, lit: `-100.23`},
		{s: `-100.`, tok: messageql.NUMBER, lit: `-100`},
		{s: `.23`, tok: messageql.NUMBER, lit: `.23`},
		{s: `+.23`, tok: messageql.NUMBER, lit: `+.23`},
		{s: `-.23`, tok: messageql.NUMBER, lit: `-.23`},
		//{s: `.`, tok: messageql.ILLEGAL, lit: `.`},
		{s: `-.`, tok: messageql.SUB, lit: ``},
		{s: `+.`, tok: messageql.ADD, lit: ``},
		{s: `10.3s`, tok: messageql.NUMBER, lit: `10.3`},

		// Durations
		{s: `10u`, tok: messageql.DURATION_VAL, lit: `10u`},
		{s: `10µ`, tok: messageql.DURATION_VAL, lit: `10µ`},
		{s: `10ms`, tok: messageql.DURATION_VAL, lit: `10ms`},
		{s: `-1s`, tok: messageql.DURATION_VAL, lit: `-1s`},
		{s: `10m`, tok: messageql.DURATION_VAL, lit: `10m`},
		{s: `10h`, tok: messageql.DURATION_VAL, lit: `10h`},
		{s: `10d`, tok: messageql.DURATION_VAL, lit: `10d`},
		{s: `10w`, tok: messageql.DURATION_VAL, lit: `10w`},
		{s: `10x`, tok: messageql.NUMBER, lit: `10`}, // non-duration unit

		// Keywords
		{s: `ALL`, tok: messageql.ALL},
		{s: `ALTER`, tok: messageql.ALTER},
		{s: `AS`, tok: messageql.AS},
		{s: `ASC`, tok: messageql.ASC},
		{s: `BEGIN`, tok: messageql.BEGIN},
		{s: `BY`, tok: messageql.BY},
		{s: `CREATE`, tok: messageql.CREATE},
		{s: `CONVERSATION`, tok: messageql.CONVERSATION},
		{s: `CONVERSATIONS`, tok: messageql.CONVERSATIONS},
		{s: `DATABASE`, tok: messageql.DATABASE},
		{s: `DATABASES`, tok: messageql.DATABASES},
		{s: `DEFAULT`, tok: messageql.DEFAULT},
		{s: `DELETE`, tok: messageql.DELETE},
		{s: `DESC`, tok: messageql.DESC},
		{s: `DROP`, tok: messageql.DROP},
		{s: `DURATION`, tok: messageql.DURATION},
		{s: `END`, tok: messageql.END},
		{s: `EXISTS`, tok: messageql.EXISTS},
		{s: `EXPLAIN`, tok: messageql.EXPLAIN},
		{s: `FIELD`, tok: messageql.FIELD},
		{s: `FROM`, tok: messageql.FROM},
		{s: `GRANT`, tok: messageql.GRANT},
		{s: `IF`, tok: messageql.IF},
		// {s: `INNER`, tok: messageql.INNER},
		{s: `INSERT`, tok: messageql.INSERT},
		{s: `KEY`, tok: messageql.KEY},
		{s: `KEYS`, tok: messageql.KEYS},
		{s: `LIMIT`, tok: messageql.LIMIT},
		{s: `SHOW`, tok: messageql.SHOW},
		{s: `MEMBER`, tok: messageql.MEMBER},
		{s: `MEMBERS`, tok: messageql.MEMBERS},
		{s: `OFFSET`, tok: messageql.OFFSET},
		{s: `ON`, tok: messageql.ON},
		{s: `ORDER`, tok: messageql.ORDER},
		{s: `ORGANIZATION`, tok: messageql.ORGANIZATION},
		{s: `ORGANIZATIONS`, tok: messageql.ORGANIZATIONS},
		{s: `PASSWORD`, tok: messageql.PASSWORD},
		{s: `POLICY`, tok: messageql.POLICY},
		{s: `POLICIES`, tok: messageql.POLICIES},
		{s: `PRIVILEGES`, tok: messageql.PRIVILEGES},
		{s: `QUERIES`, tok: messageql.QUERIES},
		{s: `QUERY`, tok: messageql.QUERY},
		{s: `READ`, tok: messageql.READ},
		{s: `RETENTION`, tok: messageql.RETENTION},
		{s: `REVOKE`, tok: messageql.REVOKE},
		{s: `SELECT`, tok: messageql.SELECT},
		{s: `TAG`, tok: messageql.TAG},
		{s: `TO`, tok: messageql.TO},
		{s: `USER`, tok: messageql.USER},
		{s: `USERS`, tok: messageql.USERS},
		{s: `VALUES`, tok: messageql.VALUES},
		{s: `WHERE`, tok: messageql.WHERE},
		{s: `WITH`, tok: messageql.WITH},
		{s: `WRITE`, tok: messageql.WRITE},
		{s: `explain`, tok: messageql.EXPLAIN}, // case insensitive
		{s: `seLECT`, tok: messageql.SELECT},   // case insensitive
	}

	for i, tt := range tests {
		s := messageql.NewScanner(strings.NewReader(tt.s))
		tok, pos, lit := s.Scan()
		if tt.tok != tok {
			t.Errorf("%d. %q token mismatch: exp=%q got=%q <%q>", i, tt.s, tt.tok, tok, lit)
		} else if tt.pos.Line != pos.Line || tt.pos.Char != pos.Char {
			t.Errorf("%d. %q pos mismatch: exp=%#v got=%#v", i, tt.s, tt.pos, pos)
		} else if tt.lit != lit {
			t.Errorf("%d. %q literal mismatch: exp=%q got=%q", i, tt.s, tt.lit, lit)
		}
	}
}

// Ensure the scanner can scan a series of tokens correctly.
func TestScanner_Scan_Multi(t *testing.T) {
	type result struct {
		tok messageql.Token
		pos messageql.Pos
		lit string
	}
	exp := []result{
		{tok: messageql.SELECT, pos: messageql.Pos{Line: 0, Char: 0}, lit: ""},
		{tok: messageql.WS, pos: messageql.Pos{Line: 0, Char: 6}, lit: " "},
		{tok: messageql.IDENT, pos: messageql.Pos{Line: 0, Char: 7}, lit: "value"},
		{tok: messageql.WS, pos: messageql.Pos{Line: 0, Char: 12}, lit: " "},
		{tok: messageql.FROM, pos: messageql.Pos{Line: 0, Char: 13}, lit: ""},
		{tok: messageql.WS, pos: messageql.Pos{Line: 0, Char: 17}, lit: " "},
		{tok: messageql.IDENT, pos: messageql.Pos{Line: 0, Char: 18}, lit: "myseries"},
		{tok: messageql.WS, pos: messageql.Pos{Line: 0, Char: 26}, lit: " "},
		{tok: messageql.WHERE, pos: messageql.Pos{Line: 0, Char: 27}, lit: ""},
		{tok: messageql.WS, pos: messageql.Pos{Line: 0, Char: 32}, lit: " "},
		{tok: messageql.IDENT, pos: messageql.Pos{Line: 0, Char: 33}, lit: "a"},
		{tok: messageql.WS, pos: messageql.Pos{Line: 0, Char: 34}, lit: " "},
		{tok: messageql.EQ, pos: messageql.Pos{Line: 0, Char: 35}, lit: ""},
		{tok: messageql.WS, pos: messageql.Pos{Line: 0, Char: 36}, lit: " "},
		{tok: messageql.STRING, pos: messageql.Pos{Line: 0, Char: 36}, lit: "b"},
		{tok: messageql.EOF, pos: messageql.Pos{Line: 0, Char: 40}, lit: ""},
	}

	// Create a scanner.
	v := `SELECT value from myseries WHERE a = 'b'`
	s := messageql.NewScanner(strings.NewReader(v))

	// Continually scan until we reach the end.
	var act []result
	for {
		tok, pos, lit := s.Scan()
		act = append(act, result{tok, pos, lit})
		if tok == messageql.EOF {
			break
		}
	}

	// Verify the token counts match.
	if len(exp) != len(act) {
		t.Fatalf("token count mismatch: exp=%d, got=%d", len(exp), len(act))
	}

	// Verify each token matches.
	for i := range exp {
		if !reflect.DeepEqual(exp[i], act[i]) {
			t.Fatalf("%d. token mismatch:\n\nexp=%#v\n\ngot=%#v", i, exp[i], act[i])
		}
	}
}

// Ensure the library can correctly scan strings.
func TestScanString(t *testing.T) {
	var tests = []struct {
		in  string
		out string
		err string
	}{
		{in: `""`, out: ``},
		{in: `"foo bar"`, out: `foo bar`},
		{in: `'foo bar'`, out: `foo bar`},
		{in: `"foo\nbar"`, out: "foo\nbar"},
		{in: `"foo\\bar"`, out: `foo\bar`},
		{in: `"foo\"bar"`, out: `foo"bar`},

		{in: `"foo` + "\n", out: `foo`, err: "bad string"}, // newline in string
		{in: `"foo`, out: `foo`, err: "bad string"},        // unclosed quotes
		{in: `"foo\xbar"`, out: `\x`, err: "bad escape"},   // invalid escape
	}

	for i, tt := range tests {
		out, err := messageql.ScanString(strings.NewReader(tt.in))
		if tt.err != errstring(err) {
			t.Errorf("%d. %s: error: exp=%s, got=%s", i, tt.in, tt.err, err)
		} else if tt.out != out {
			t.Errorf("%d. %s: out: exp=%s, got=%s", i, tt.in, tt.out, out)
		}
	}
}

// Test scanning regex
func TestScanRegex(t *testing.T) {
	var tests = []struct {
		in  string
		tok messageql.Token
		lit string
		err string
	}{
		{in: `/^payments\./`, tok: messageql.REGEX, lit: `^payments\.`},
		{in: `/foo\/bar/`, tok: messageql.REGEX, lit: `foo/bar`},
		{in: `/foo\\/bar/`, tok: messageql.REGEX, lit: `foo\/bar`},
		{in: `/foo\\bar/`, tok: messageql.REGEX, lit: `foo\\bar`},
	}

	for i, tt := range tests {
		s := messageql.NewScanner(strings.NewReader(tt.in))
		tok, _, lit := s.ScanRegex()
		if tok != tt.tok {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.tok.String(), tok.String())
		}
		if lit != tt.lit {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.lit, lit)
		}
	}
}
