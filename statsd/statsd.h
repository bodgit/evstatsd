/*
 * Copyright (c) 2013 Matt Dainty <matt@bodgit-n-scarper.com>
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

#include <sys/socket.h>
#include <sys/queue.h>
#include <sys/param.h>

#include <arpa/inet.h>

#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include <event2/event.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/http.h>

#include "common.h"
#include "graphite.h"

#define	STATSD_CONF_FILE		"/etc/statsd.conf"
#define	STATSD_USER			"_statsd"

#define	STATSD_DEFAULT_STATS_PORT	8125
#define	STATSD_DEFAULT_HTTP_PORT	8126

#define	STATSD_HASH_SIZE		65535

#define	STATSD_MAX_UDP_PACKET		8192

#define	STATSD_GRAPHITE_CONNECTED	(1 << 0)

enum statistic_type {
	STATSD_COUNTER = 0,
	STATSD_GAUGE,
	STATSD_MAX_TYPE
};

struct statistic {
	TAILQ_ENTRY(statistic)		 entry;
	char				*metric;
	struct timeval			 tv;
	enum statistic_type		 type;
	union {
		long double		 count;
	} value;
};

struct chain {
	TAILQ_ENTRY(chain)		 entry;
	struct statistic		*stat;
};

struct listen_addr {
	TAILQ_ENTRY(listen_addr)	 entry;
	struct sockaddr_storage		 sa;
	int				 port;
	int				 fd;
	struct event			*ev;
};

struct statsd_addr {
	struct statsd_addr	*next;
	struct sockaddr_storage	 ss;
};

struct statsd_addr_wrap {
	char			*name;
	struct statsd_addr	*a;
};

struct statsd {
	struct event_base			*base;

	int					 state;

	TAILQ_HEAD(listen_addrs, listen_addr)	 listen_addrs;

	char					*graphite_host;
	unsigned short				 graphite_port;
	struct timeval				 graphite_reconnect;
	struct timeval				 graphite_interval;

	struct graphite_connection		*graphite_conn;
	struct event				*graphite_ev;

	char					*stats_host;
	unsigned short				 stats_port;
	struct timeval				 stats_reconnect;
	struct timeval				 stats_interval;
	char					*stats_prefix;

	struct graphite_connection		*stats_conn;
	struct event				*stats_ev;

	struct evhttp				*httpd;

	TAILQ_HEAD(statistics, statistic)	 stats;
	TAILQ_HEAD(chains, chain)		 chains[STATSD_HASH_SIZE];

	unsigned long long			 count[STATSD_MAX_TYPE];
};

/* prototypes */
/* parse.y */
struct statsd	*parse_config(const char *, int);
int		 host(const char *, struct statsd_addr **);
int		 host_dns(const char *, struct statsd_addr **);
