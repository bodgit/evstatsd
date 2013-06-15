/*
 * Copyright (c) 2013 Matt Dainty <matt@bodgit-n-scarper.com>
 * Copyright (c) 2002, 2003, 2004 Henning Brauer <henning@openbsd.org>
 * Copyright (c) 2001 Markus Friedl.  All rights reserved.
 * Copyright (c) 2001 Daniel Hartmeier.  All rights reserved.
 * Copyright (c) 2001 Theo de Raadt.  All rights reserved.
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

%{
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/param.h>

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <unistd.h>

#include "statsd.h"

TAILQ_HEAD(files, file)		 files = TAILQ_HEAD_INITIALIZER(files);
static struct file {
	TAILQ_ENTRY(file)	 entry;
	FILE			*stream;
	char			*name;
	int			 lineno;
	int			 errors;
} *file, *topfile;
struct file	*pushfile(const char *);
int		 popfile(void);
int		 yyparse(void);
int		 yylex(void);
int		 yyerror(const char *, ...);
int		 kw_cmp(const void *, const void *);
int		 lookup(char *);
int		 lgetc(int);
int		 lungetc(int);
int		 findeol(void);

struct statsd		*conf;

struct opts {
	int		 port;
	int		 reconnect;
	char		*prefix;
	int		 interval;
	int		 flags;
} opts;
void		 opts_default(void);

/* FIXME */
#define YYSTYPE_IS_DECLARED 1
typedef struct {
	union {
		int64_t				 number;
		char				*string;
		struct statsd_addr_wrap		*addr;
		struct opts			 opts;
	} v;
	int lineno;
} YYSTYPE;

%}

%token	LISTEN ON
%token	GRAPHITE
%token	STATISTICS INTERVAL PREFIX
%token	PORT
%token	RECONNECT
%token	ERROR
%token	<v.string>		STRING
%token	<v.number>		NUMBER
%type	<v.addr>		address
%type	<v.opts>		listen_opts listen_opts_l listen_opt
%type	<v.opts>		graphite_opts graphite_opts_l graphite_opt
%type	<v.opts>		stats_opts stats_opts_l stats_opt
%type	<v.opts>		port
%type	<v.opts>		reconnect
%type	<v.opts>		interval
%type	<v.opts>		prefix
%%

grammar		: /* empty */
		| grammar '\n'
		| grammar main '\n'
		| grammar error '\n'		{ file->errors++; }
		;

main		: LISTEN ON address listen_opts	{
			struct listen_addr	*la;
			struct statsd_addr	*h, *next;

			if ((h = $3->a) == NULL &&
			    (host_dns($3->name, &h) == -1 || !h)) {
				yyerror("could not resolve \"%s\"", $3->name);
				free($3->name);
				free($3);
				YYERROR;
			}

			for (; h != NULL; h = next) {
				next = h->next;
				la = calloc(1, sizeof(struct listen_addr));
				if (la == NULL)
					fatal("listen on calloc");
				la->port =
				    (opts.port) ? opts.port : GRAPHITE_DEFAULT_PORT;
				memcpy(&la->sa, &h->ss,
				    sizeof(struct sockaddr_storage));
				TAILQ_INSERT_TAIL(&conf->listen_addrs, la,
				    entry);
				free(h);
			}
			free($3->name);
			free($3);
		}
		| GRAPHITE STRING graphite_opts	{
			if (conf->graphite_host)
				free(conf->graphite_host);
			conf->graphite_host = $2;
			conf->graphite_port = opts.port;
			conf->graphite_reconnect.tv_sec = opts.reconnect;
			conf->stats_interval.tv_sec = opts.interval;
		}
		| STATISTICS STRING stats_opts	{
			if (conf->stats_host)
				free(conf->stats_host);
			conf->stats_host = $2;
			conf->stats_port = opts.port;
			conf->stats_reconnect.tv_sec = opts.reconnect;
			conf->stats_interval.tv_sec = opts.interval;
			if (conf->stats_prefix)
				free(conf->stats_prefix);
			conf->stats_prefix = opts.prefix;
		}
		;

address		: STRING		{
			if (($$ = calloc(1, sizeof(struct statsd_addr_wrap))) ==
			    NULL)
				fatal(NULL);
			if (host($1, &$$->a) == -1) {
				yyerror("could not parse address spec \"%s\"",
				    $1);
				free($1);
				free($$);
				YYERROR;
			}
			$$->name = $1;
		}
		;

listen_opts	:	{ opts_default(); }
		  listen_opts_l
			{ $$ = opts; }
		|	{ opts_default(); $$ = opts; }
		;
listen_opts_l	: listen_opts_l listen_opt
		| listen_opt
		;
listen_opt	: port
		;

graphite_opts	:	{ opts_default(); }
		  graphite_opts_l
			{ $$ = opts; }
		|	{ opts_default(); $$ = opts; }
		;
graphite_opts_l	: graphite_opts_l graphite_opt
		| graphite_opt
		;
graphite_opt	: port
		| reconnect
		| interval
		;

stats_opts	:	{ opts_default(); }
		  stats_opts_l
			{ $$ = opts; }
		|	{ opts_default(); $$ = opts; }
		;
stats_opts_l	: stats_opts_l stats_opt
		| stats_opt
		;
stats_opt	: port
		| reconnect
		| interval
		| prefix
		;

port		: PORT NUMBER {
			if ($2 < 0 || $2 > USHRT_MAX) {
				yyerror("invalid port number");
				YYERROR;
			}
			opts.port = $2;
		}
		;

reconnect	: RECONNECT NUMBER {
			if ($2 < 0 || $2 > UINT_MAX) {
				yyerror("invalid reconnect");
				YYERROR;
			}
			opts.reconnect = $2;
		}
		;

interval	: INTERVAL NUMBER {
			if ($2 < 0 || $2 > UINT_MAX) {
				yyerror("invalid interval");
				YYERROR;
			}
			opts.interval = $2;
		}
		;

prefix		: PREFIX STRING {
			opts.prefix = $2;
		}
		;

%%

void
opts_default(void)
{
	bzero(&opts, sizeof opts);
}

struct keywords {
	const char	*k_name;
	int		 k_val;
};

int
yyerror(const char *fmt, ...)
{
	va_list		 ap;
	char		*nfmt;

	file->errors++;
	va_start(ap, fmt);
	if (asprintf(&nfmt, "%s:%d: %s", file->name, yylval.lineno, fmt) == -1)
		fatalx("yyerror asprintf");
	vlog(LOG_CRIT, nfmt, ap);
	va_end(ap);
	free(nfmt);
	return (0);
}

int
kw_cmp(const void *k, const void *e)
{
	return (strcmp(k, ((const struct keywords *)e)->k_name));
}

int
lookup(char *s)
{
	/* this has to be sorted always */
	static const struct keywords keywords[] = {
		{ "graphite",		GRAPHITE},
		{ "interval",		INTERVAL},
		{ "listen",		LISTEN},
		{ "on",			ON},
		{ "port",		PORT},
		{ "prefix",		PREFIX},
		{ "reconnect",		RECONNECT},
		{ "statistics",		STATISTICS}
	};
	const struct keywords	*p;

	p = bsearch(s, keywords, sizeof(keywords)/sizeof(keywords[0]),
	    sizeof(keywords[0]), kw_cmp);

	if (p)
		return (p->k_val);
	else
		return (STRING);
}

#define MAXPUSHBACK	128

char	*parsebuf;
int	 parseindex;
char	 pushback_buffer[MAXPUSHBACK];
int	 pushback_index = 0;

int
lgetc(int quotec)
{
	int		c, next;

	if (parsebuf) {
		/* Read character from the parsebuffer instead of input. */
		if (parseindex >= 0) {
			c = parsebuf[parseindex++];
			if (c != '\0')
				return (c);
			parsebuf = NULL;
		} else
			parseindex++;
	}

	if (pushback_index)
		return (pushback_buffer[--pushback_index]);

	if (quotec) {
		if ((c = getc(file->stream)) == EOF) {
			yyerror("reached end of file while parsing "
			    "quoted string");
			if (file == topfile || popfile() == EOF)
				return (EOF);
			return (quotec);
		}
		return (c);
	}

	while ((c = getc(file->stream)) == '\\') {
		next = getc(file->stream);
		if (next != '\n') {
			c = next;
			break;
		}
		yylval.lineno = file->lineno;
		file->lineno++;
	}

	while (c == EOF) {
		if (file == topfile || popfile() == EOF)
			return (EOF);
		c = getc(file->stream);
	}
	return (c);
}

int
lungetc(int c)
{
	if (c == EOF)
		return (EOF);
	if (parsebuf) {
		parseindex--;
		if (parseindex >= 0)
			return (c);
	}
	if (pushback_index < MAXPUSHBACK-1)
		return (pushback_buffer[pushback_index++] = c);
	else
		return (EOF);
}

int
findeol(void)
{
	int	c;

	parsebuf = NULL;

	/* skip to either EOF or the first real EOL */
	while (1) {
		if (pushback_index)
			c = pushback_buffer[--pushback_index];
		else
			c = lgetc(0);
		if (c == '\n') {
			file->lineno++;
			break;
		}
		if (c == EOF)
			break;
	}
	return (ERROR);
}

int
yylex(void)
{
	char	 buf[8096];
	char	*p;
	int	 quotec, next, c;
	int	 token;

	p = buf;
	while ((c = lgetc(0)) == ' ' || c == '\t')
		; /* nothing */

	yylval.lineno = file->lineno;
	if (c == '#')
		while ((c = lgetc(0)) != '\n' && c != EOF)
			; /* nothing */

	switch (c) {
	case '\'':
	case '"':
		quotec = c;
		while (1) {
			if ((c = lgetc(quotec)) == EOF)
				return (0);
			if (c == '\n') {
				file->lineno++;
				continue;
			} else if (c == '\\') {
				if ((next = lgetc(quotec)) == EOF)
					return (0);
				if (next == quotec || c == ' ' || c == '\t')
					c = next;
				else if (next == '\n') {
					file->lineno++;
					continue;
				} else
					lungetc(next);
			} else if (c == quotec) {
				*p = '\0';
				break;
			}
			if (p + 1 >= buf + sizeof(buf) - 1) {
				yyerror("string too long");
				return (findeol());
			}
			*p++ = (char)c;
		}
		yylval.v.string = strdup(buf);
		if (yylval.v.string == NULL)
			fatal("yylex: strdup");
		return (STRING);
	}

#define allowed_to_end_number(x) \
	(isspace(x) || x == ')' || x ==',' || x == '/' || x == '}' || x == '=')

	if (c == '-' || isdigit(c)) {
		do {
			*p++ = c;
			if ((unsigned)(p-buf) >= sizeof(buf)) {
				yyerror("string too long");
				return (findeol());
			}
		} while ((c = lgetc(0)) != EOF && isdigit(c));
		lungetc(c);
		if (p == buf + 1 && buf[0] == '-')
			goto nodigits;
		if (c == EOF || allowed_to_end_number(c)) {
			const char *errstr = NULL;

			*p = '\0';
			yylval.v.number = strtonum(buf, LLONG_MIN,
			    LLONG_MAX, &errstr);
			if (errstr) {
				yyerror("\"%s\" invalid number: %s",
				    buf, errstr);
				return (findeol());
			}
			return (NUMBER);
		} else {
nodigits:
			while (p > buf + 1)
				lungetc(*--p);
			c = *--p;
			if (c == '-')
				return (c);
		}
	}

#define allowed_in_string(x) \
	(isalnum(x) || (ispunct(x) && x != '(' && x != ')' && \
	x != '{' && x != '}' && x != '<' && x != '>' && \
	x != '!' && x != '=' && x != '/' && x != '#' && \
	x != ','))

	if (isalnum(c) || c == ':' || c == '_' || c == '*') {
		do {
			*p++ = c;
			if ((unsigned)(p-buf) >= sizeof(buf)) {
				yyerror("string too long");
				return (findeol());
			}
		} while ((c = lgetc(0)) != EOF && (allowed_in_string(c)));
		lungetc(c);
		*p = '\0';
		if ((token = lookup(buf)) == STRING)
			if ((yylval.v.string = strdup(buf)) == NULL)
				fatal("yylex: strdup");
		return (token);
	}
	if (c == '\n') {
		yylval.lineno = file->lineno;
		file->lineno++;
	}
	if (c == EOF)
		return (0);
	return (c);
}

struct file *
pushfile(const char *name)
{
	struct file	*nfile;

	if ((nfile = calloc(1, sizeof(struct file))) == NULL) {
		log_warn("malloc");
		return (NULL);
	}
	if ((nfile->name = strdup(name)) == NULL) {
		log_warn("malloc");
		free(nfile);
		return (NULL);
	}
	if ((nfile->stream = fopen(nfile->name, "r")) == NULL) {
		log_warn("%s", nfile->name);
		free(nfile->name);
		free(nfile);
		return (NULL);
	}
	nfile->lineno = 1;
	TAILQ_INSERT_TAIL(&files, nfile, entry);
	return (nfile);
}

int
popfile(void)
{
	struct file	*prev;

	if ((prev = TAILQ_PREV(file, files, entry)) != NULL)
		prev->errors += file->errors;

	TAILQ_REMOVE(&files, file, entry);
	fclose(file->stream);
	free(file->name);
	free(file);
	file = prev;
	return (file ? 0 : EOF);
}

struct statsd *
parse_config(const char *filename, int flags)
{
	int	 errors = 0;
	char	 hostname[MAXHOSTNAMELEN];
	char	*ptr;
	size_t	 size;

	if ((conf = calloc(1, sizeof(*conf))) == NULL) {
		log_warn("cannot allocate memory");
		return (NULL);
	}

	TAILQ_INIT(&conf->listen_addrs);
	RB_INIT(&conf->stats);

	if ((file = pushfile(filename)) == NULL) {
		free(conf);
		return (NULL);
	}
	topfile = file;

	yyparse();
	errors = file->errors;
	popfile();

	if (errors) {
		free(conf);
		return (NULL);
	}

	/* Fill in the gaps with well-defined defaults
	 */

	/* Graphite */
	if (conf->graphite_host == NULL)
		if ((conf->graphite_host = strdup(GRAPHITE_DEFAULT_HOST)) == NULL)
			fatal("strdup");
	if (conf->graphite_port == 0)
		conf->graphite_port = GRAPHITE_DEFAULT_PORT;
	if (conf->graphite_reconnect.tv_sec == 0)
		conf->graphite_reconnect.tv_sec = 10;
	if (conf->graphite_interval.tv_sec == 0)
		conf->graphite_interval.tv_sec = 60;

	/* Statistics */
	if (conf->stats_host == NULL)
		if ((conf->stats_host = strdup(GRAPHITE_DEFAULT_HOST)) == NULL)
			fatal("strdup");
	if (conf->stats_port == 0)
		conf->stats_port = GRAPHITE_DEFAULT_PORT;
	if (conf->stats_reconnect.tv_sec == 0)
		conf->stats_reconnect.tv_sec = 10;
	if (conf->stats_interval.tv_sec == 0)
		conf->stats_interval.tv_sec = 60;
	if (conf->stats_prefix == NULL) {
		gethostname(hostname, MAXHOSTNAMELEN);
		while ((ptr = strchr(hostname, '.')) != NULL)
			*ptr = '_';
		size = snprintf(NULL, 0, "%s.statsd", hostname);
		conf->stats_prefix = calloc(size + 1, sizeof(char));
		sprintf(conf->stats_prefix, "%s.statsd", hostname);
	}

	return (conf);
}

struct statsd_addr	*host_v4(const char *);
struct statsd_addr	*host_v6(const char *);

int
host(const char *s, struct statsd_addr **hn)
{
	struct statsd_addr *h = NULL;

	if (!strcmp(s, "*"))
		if ((h = calloc(1, sizeof(struct statsd_addr))) == NULL)
			fatal(NULL);

	/* IPv4 address? */
	if (h == NULL)
		h = host_v4(s);

	/* IPv6 address? */
	if (h == NULL)
		h = host_v6(s);

	if (h == NULL)
		return (0);

	*hn = h;

	return (1);
}

struct statsd_addr *
host_v4(const char *s)
{
	struct in_addr		 ina;
	struct sockaddr_in	*sa_in;
	struct statsd_addr	*h;

	bzero(&ina, sizeof(struct in_addr));
	if (inet_pton(AF_INET, s, &ina) != 1)
		return (NULL);

	if ((h = calloc(1, sizeof(struct statsd_addr))) == NULL)
		fatal(NULL);
	sa_in = (struct sockaddr_in *)&h->ss;
#ifdef HAVE_STRUCT_SOCKADDR_IN_SIN_LEN
	sa_in->sin_len = sizeof(struct sockaddr_in);
#endif
	sa_in->sin_family = AF_INET;
	sa_in->sin_addr.s_addr = ina.s_addr;

	return (h);
}

struct statsd_addr *
host_v6(const char *s)
{
	struct addrinfo		 hints, *res;
	struct sockaddr_in6	*sa_in6;
	struct statsd_addr	*h = NULL;

	bzero(&hints, sizeof(hints));
	hints.ai_family = AF_INET6;
	hints.ai_socktype = SOCK_DGRAM;	/* DUMMY */
	hints.ai_flags = AI_NUMERICHOST;
	if (getaddrinfo(s, "0", &hints, &res) == 0) {
		if ((h = calloc(1, sizeof(struct statsd_addr))) == NULL)
			fatal(NULL);
		sa_in6 = (struct sockaddr_in6 *)&h->ss;
#ifdef HAVE_STRUCT_SOCKADDR_IN6_SIN6_LEN
		sa_in6->sin6_len = sizeof(struct sockaddr_in6);
#endif
		sa_in6->sin6_family = AF_INET6;
		memcpy(&sa_in6->sin6_addr,
		    &((struct sockaddr_in6 *)res->ai_addr)->sin6_addr,
		    sizeof(sa_in6->sin6_addr));
		sa_in6->sin6_scope_id =
		    ((struct sockaddr_in6 *)res->ai_addr)->sin6_scope_id;

		freeaddrinfo(res);
	}

	return (h);
}

#define	MAX_SERVERS_DNS	8

int
host_dns(const char *s, struct statsd_addr **hn)
{
	struct addrinfo		 hints, *res0, *res;
	int			 error, cnt = 0;
	struct sockaddr_in	*sa_in;
	struct sockaddr_in6	*sa_in6;
	struct statsd_addr	*h, *hh = NULL;

	bzero(&hints, sizeof(hints));
	hints.ai_family = PF_UNSPEC;
	hints.ai_socktype = SOCK_DGRAM;	/* DUMMY */
	error = getaddrinfo(s, NULL, &hints, &res0);
	if (error == EAI_AGAIN || error == EAI_NODATA || error == EAI_NONAME)
		return (0);
	if (error) {
		log_warnx("could not parse \"%s\": %s", s,
		    gai_strerror(error));
		return (-1);
	}

	for (res = res0; res && cnt < MAX_SERVERS_DNS; res = res->ai_next) {
		if (res->ai_family != AF_INET &&
		    res->ai_family != AF_INET6)
			continue;
		if ((h = calloc(1, sizeof(struct statsd_addr))) == NULL)
			fatal(NULL);
		h->ss.ss_family = res->ai_family;
		if (res->ai_family == AF_INET) {
			sa_in = (struct sockaddr_in *)&h->ss;
#ifdef HAVE_STRUCT_SOCKADDR_IN_SIN_LEN
			sa_in->sin_len = sizeof(struct sockaddr_in);
#endif
			sa_in->sin_addr.s_addr = ((struct sockaddr_in *)
			    res->ai_addr)->sin_addr.s_addr;
		} else {
			sa_in6 = (struct sockaddr_in6 *)&h->ss;
#ifdef HAVE_STRUCT_SOCKADDR_IN6_SIN6_LEN
			sa_in6->sin6_len = sizeof(struct sockaddr_in6);
#endif
			memcpy(&sa_in6->sin6_addr, &((struct sockaddr_in6 *)
			    res->ai_addr)->sin6_addr, sizeof(struct in6_addr));
		}

		h->next = hh;
		hh = h;
		cnt++;
	}
	freeaddrinfo(res0);

	*hn = hh;
	return (cnt);
}
