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
#include <err.h>
#include <signal.h>

#include <event2/event.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/http.h>

#include "statsd.h"

#define	STATSD_DEFAULT_STATS_PORT	8125
#define	STATSD_DEFAULT_HTTP_PORT	8126

#define	STATSD_HASH_SIZE		65535

#define	STATSD_MAX_UDP_PACKET		8192

#define	STATSD_GRAPHITE_CONNECTED	(1 << 0)

struct statistic_dispatch {
	char	 *path;
	void	(*single_cb)(struct evhttp_request *, void *);
	void	(*list_cb)(struct evhttp_request *, void *);
};

__dead void	 usage(void);
unsigned long	 hash(unsigned char *str);
void		 stats_timer_cb(int, short, void *);
void		 stats_connect_cb(struct graphite_connection *, void *);
void		 stats_disconnect_cb(struct graphite_connection *, void *);
void		 graphite_connect_cb(struct graphite_connection *, void *);
void		 graphite_disconnect_cb(struct graphite_connection *, void *);
void		 graphite_timer_cb(int, short, void *);
void		 process_generic_list(struct evhttp_request *, void *,
		    enum statistic_type);
void		 process_counter_list(struct evhttp_request *, void *);
void		 process_gauge_list(struct evhttp_request *, void *);
void		 process_generic(struct evhttp_request *, void *,
		    enum statistic_type);
void		 process_counter(struct evhttp_request *, void *);
void		 process_gauge(struct evhttp_request *, void *);
void		 statsd_read_cb(int, short, void *);
void		 handle_signal(int, short, void *);

struct statistic_dispatch dispatch[STATSD_MAX_TYPE] = {
	{ "counters", process_counter, process_counter_list },
	{ "gauges",   process_gauge,   process_gauge_list   }
};

__dead void
usage(void)
{
	extern char	*__progname;

	fprintf(stderr, "usage: %s [-dnv] [-f file]\n", __progname);
	exit(1);
}

/* djb2 */
unsigned long
hash(unsigned char *str)
{
	unsigned long	 hash = 5381;
	int		 c;

	while ((c = *str++))
		hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

	return (hash);
}

void
stats_timer_cb(int fd, short event, void *arg)
{
	struct statsd	*env = (struct statsd *)arg;
	struct timeval	 tv;
	int		 i;

	gettimeofday(&tv, NULL);

	graphite_send_metric(env->stats_conn, env->stats_prefix,
	    "graphite.bytes.tx", tv, "%lld", env->graphite_conn->bytes_tx);
	graphite_send_metric(env->stats_conn, env->stats_prefix,
	    "graphite.metrics.tx", tv, "%lld", env->graphite_conn->metrics_tx);
	graphite_send_metric(env->stats_conn, env->stats_prefix,
	    "graphite.buffer.input", tv, "%zd",
	    (env->graphite_conn->bev == NULL) ? 0 :
	    evbuffer_get_length(bufferevent_get_input(env->graphite_conn->bev)));
	graphite_send_metric(env->stats_conn, env->stats_prefix,
	    "graphite.buffer.output", tv, "%zd",
	    (env->graphite_conn->bev == NULL) ? 0 :
	    evbuffer_get_length(bufferevent_get_output(env->graphite_conn->bev)));

	for (i = 0; i < STATSD_MAX_TYPE; i++)
		graphite_send_metric(env->stats_conn, env->stats_prefix,
		    dispatch[i].path, tv, "%lld", env->count[i]);
}

void
stats_connect_cb(struct graphite_connection *c, void *arg)
{
	struct statsd	*env = (struct statsd *)arg;

	log_debug("Connected to %s:%hu", env->stats_host,
	    env->stats_port);
	evtimer_add(env->stats_ev, &env->stats_interval);
}

void
stats_disconnect_cb(struct graphite_connection *c, void *arg)
{
	struct statsd	*env = (struct statsd *)arg;

	log_debug("Disconnected from %s:%hu", env->stats_host,
	    env->stats_port);
	if (evtimer_pending(env->stats_ev, NULL))
		evtimer_del(env->stats_ev);
}

void
graphite_connect_cb(struct graphite_connection *c, void *arg)
{
	struct statsd	*env = (struct statsd *)arg;

	log_debug("Connected to %s:%hu", env->graphite_host,
	    env->graphite_port);
	env->state |= STATSD_GRAPHITE_CONNECTED;
}

void
graphite_disconnect_cb(struct graphite_connection *c, void *arg)
{
	struct statsd	*env = (struct statsd *)arg;

	log_debug("Disconnected from %s:%hu", env->graphite_host,
	    env->graphite_port);
	env->state &= ~(STATSD_GRAPHITE_CONNECTED);
}

void
graphite_timer_cb(int fd, short event, void *arg)
{
	struct statsd		*env = (struct statsd *)arg;
	struct statistic	*stat;
	struct timeval		 tv;

	gettimeofday(&tv, NULL);

	for (stat = TAILQ_FIRST(&env->stats); stat;
	    stat = TAILQ_NEXT(stat, entry)) {
		switch (stat->type) {
		case STATSD_COUNTER:
			/* FALLTHROUGH */
		case STATSD_GAUGE:
			log_debug("Sending %s = %Lf to graphite", stat->metric,
			    stat->value.count);
			if (env->state & STATSD_GRAPHITE_CONNECTED) {
				graphite_send_metric(env->graphite_conn,
				    NULL, stat->metric, tv, "%Lf",
				    stat->value.count);
			}
			if (stat->type == STATSD_COUNTER)
				stat->value.count = 0;
			break;
		default:
			break;
		}
	}
}

void
process_generic_list(struct evhttp_request *req, void *arg,
    enum statistic_type type)
{
	struct statsd		*env = (struct statsd *)arg;
	struct evbuffer		*buf;
	struct statistic	*stat;

	switch (evhttp_request_get_command(req)) {
	case EVHTTP_REQ_GET:
		if ((buf = evbuffer_new()) == NULL)
			return;
		evbuffer_add_printf(buf, "[");
		for (stat = TAILQ_FIRST(&env->stats); stat;
		    stat = TAILQ_NEXT(stat, entry)) {
			/* Not the type we're interested in, next! */
			if (stat->type != type)
				continue;
			evbuffer_add_printf(buf, "\"%s\"", stat->metric);
			if (TAILQ_NEXT(stat, entry))
				evbuffer_add_printf(buf, ",");
		}
		evbuffer_add_printf(buf, "]\n");
		evhttp_add_header(evhttp_request_get_output_headers(req),
		    "Content-Type", "application/json");
		evhttp_send_reply(req, HTTP_OK, "OK", buf);
		evbuffer_free(buf);
		break;
	default:
		evhttp_add_header(evhttp_request_get_output_headers(req),
		    "Allow", "GET");
#if 0
		/* evhttp_send_error() doesn't appear to honour any
		 * additional headers set, unlike evhttp_send_reply(). RFC
		 * states we should send back Allow: header.
		 */
		evhttp_send_error(req, HTTP_BADMETHOD, "Bad Method");
#else
		evhttp_send_reply(req, HTTP_BADMETHOD, "Bad Method", NULL);
#endif
		break;
	}
}

void
process_counter_list(struct evhttp_request *req, void *arg)
{
	process_generic_list(req, arg, STATSD_COUNTER);
}

void
process_gauge_list(struct evhttp_request *req, void *arg)
{
	process_generic_list(req, arg, STATSD_GAUGE);
}

void
process_generic(struct evhttp_request *req, void *arg,
    enum statistic_type type)
{
	struct statsd	*env = (struct statsd *)arg;
	const char	*metric;
	struct chain	*chain;
	struct evbuffer	*buf;

	/* Metric is the rest of the URL after "/<type>/" */
	metric = evhttp_uri_get_path(evhttp_request_get_evhttp_uri(req)) +
	    strlen(dispatch[type].path) + 2;

	for (chain = TAILQ_FIRST(&env->chains[hash((unsigned char *)metric) %
	    STATSD_HASH_SIZE]); chain &&
	    strcmp(chain->stat->metric, metric) != 0;
	    chain = TAILQ_NEXT(chain, entry));

	/* Shouldn't ever happen */
	if (!chain)
		return;

	switch (evhttp_request_get_command(req)) {
	case EVHTTP_REQ_GET:
		if ((buf = evbuffer_new()) == NULL)
			return;
		switch (type) {
		case STATSD_COUNTER:
			/* FALLTHROUGH */
		case STATSD_GAUGE:
			evbuffer_add_printf(buf,
			    "{\"name\":\"%s\",\"value\":%Lf,\"last_modified\":%lu}\n",
			    metric, chain->stat->value.count,
			    chain->stat->tv.tv_sec);
			break;
		default:
			break;
		}
		evhttp_add_header(evhttp_request_get_output_headers(req),
		    "Content-Type", "application/json");
		evhttp_send_reply(req, HTTP_OK, "OK", buf);
		evbuffer_free(buf);
		break;
	case EVHTTP_REQ_DELETE:
		evhttp_send_reply(req, HTTP_NOCONTENT, "No Content", NULL);
		evhttp_del_cb(env->httpd, evhttp_request_get_uri(req));

		env->count[type]--;

		TAILQ_REMOVE(&env->stats, chain->stat, entry);
		TAILQ_REMOVE(&env->chains[hash((unsigned char *)metric) %
		    STATSD_HASH_SIZE], chain, entry);

		/* Some statistic types may require additional cleanup */
		switch (type) {
		default:
			break;
		}

		free(chain->stat->metric);
		free(chain->stat);
		free(chain);
	default:
		evhttp_add_header(evhttp_request_get_output_headers(req),
		    "Allow", "GET, DELETE");
#if 0
		/* evhttp_send_error() doesn't appear to honour any
		 * additional headers set, unlike evhttp_send_reply(). RFC
		 * states we should send back Allow: header.
		 */
		evhttp_send_error(req, HTTP_BADMETHOD, "Bad Method");
#else
		evhttp_send_reply(req, HTTP_BADMETHOD, "Bad Method", NULL);
#endif
		break;
	}
}

void
process_counter(struct evhttp_request *req, void *arg)
{
	process_generic(req, arg, STATSD_COUNTER);
}

void
process_gauge(struct evhttp_request *req, void *arg)
{
	process_generic(req, arg, STATSD_GAUGE);
}

void
statsd_read_cb(int fd, short event, void *arg)
{
	struct statsd		*env = (struct statsd *)arg;
	struct sockaddr_storage	 ss;
	socklen_t		 slen;
	ssize_t			 len;
	char			 storage[STATSD_MAX_UDP_PACKET];
	char			*ptr, *optr, *nptr;

	char			*metric;
	size_t			 length;

	struct statistic	*stat, *nstat;
	struct chain		*chain, *nchain;

	char			*path;
	double			 value;
	enum statistic_type	 type;

	bzero(storage, STATSD_MAX_UDP_PACKET);
	slen = sizeof(ss);
	if ((len = recvfrom(fd, storage, sizeof(storage), 0,
	    (struct sockaddr *)&ss, &slen)) < 1)
		return;

	//log_debug("Packet received: \"%s\"", storage);
	ptr = storage;
	length = strcspn(ptr, ":");
	metric = calloc(length + 1, sizeof(char));
	strncpy(metric, ptr, length);
	ptr += length;

	if (*ptr != ':') {
		log_warnx("No ':'");
		free(metric);
		return;
	}
	ptr++;

	/* Remember where the value starts for checking for +/- later */
	optr = ptr;
	if (((value = strtod(ptr, &nptr)) == 0) && (nptr == ptr)) {
		log_warnx("Bad double at %s", ptr);
		free(metric);
		return;
	}

	if (*nptr != '|') {
		log_warnx("No '|'");
		free(metric);
		return;
	}
	ptr = nptr + 1;

#if 0
	/* Counter or gauge? */
	if ((length = strspn(ptr, "cg")) != 1) {
		log_warnx("Invalid type");
		free(metric);
		return;
	}
#endif

	switch (*ptr) {
	case 'c':
		type = STATSD_COUNTER;
		break;
	case 'g':
		type = STATSD_GAUGE;
		break;
	case 'm':
		/* FALLTHROUGH */
	case 's':
		/* FALLTHROUGH */
	default:
		log_warnx("Invalid type");
		free(metric);
		return;
		/* NOTREACHED */
	}

	//log_debug("hash(%s) = %lu", metric,
	//    hash((unsigned char *)metric) % STATSD_HASH_SIZE);

	for (chain = TAILQ_FIRST(&env->chains[hash((unsigned char *)metric) %
	    STATSD_HASH_SIZE]); chain &&
	    strcmp(metric, chain->stat->metric) > 0;
	    chain = TAILQ_NEXT(chain, entry));

	/* Same metric name, different type */
	if (chain && strcmp(metric, chain->stat->metric) == 0 &&
	    chain->stat->type != type) {
		log_warnx("Metric %s already exists with different type",
		    metric);
		free(metric);
		return;
	}

	if (!chain || strcmp(metric, chain->stat->metric)) {
		log_debug("Hash miss on %s in chain %lu", metric,
		    hash((unsigned char *)metric) % STATSD_HASH_SIZE);

		nstat = calloc(1, sizeof(struct statistic));
		nstat->metric = strdup(metric);
		nstat->type = type;

		for (stat = TAILQ_FIRST(&env->stats); stat && strcmp(metric,
		    stat->metric) > 0; stat = TAILQ_NEXT(stat, entry));
		if (stat)
			TAILQ_INSERT_BEFORE(stat, nstat, entry);
		else
			TAILQ_INSERT_TAIL(&env->stats, nstat, entry);

		path = calloc(strlen(dispatch[type].path) + strlen(metric) + 3,
		    sizeof(char));
		sprintf(path, "/%s/%s", dispatch[type].path, metric);
		evhttp_set_cb(env->httpd, path, dispatch[type].single_cb,
		    (void *)env);
		free(path);

		nchain = calloc(1, sizeof(struct chain));
		nchain->stat = nstat;
		if (chain) {
			log_debug("Insert before %s in chain %lu",
			    chain->stat->metric,
			    hash((unsigned char *)metric) % STATSD_HASH_SIZE);
			TAILQ_INSERT_BEFORE(chain, nchain, entry);
		} else {
			log_debug("Insert at end of chain %lu",
			    hash((unsigned char *)metric) % STATSD_HASH_SIZE);
			TAILQ_INSERT_TAIL(&env->chains[hash((unsigned char *)metric) %
			    STATSD_HASH_SIZE], nchain, entry);
		}

		env->count[type]++;

		chain = nchain;
	}

	switch (chain->stat->type) {
	case STATSD_COUNTER:
		/* FALLTHROUGH */
	case STATSD_GAUGE:
		if (chain->stat->type == STATSD_COUNTER ||
		    (*optr == '+' || *optr == '-'))
			chain->stat->value.count += value;
		else
			chain->stat->value.count = value;
		break;
	default:
		break;
	}

	/* Record last time this metric was updated */
	gettimeofday(&chain->stat->tv, NULL);

	free(metric);
}

void
handle_signal(int sig, short event, void *arg)
{
#if 0
	struct statsd		*env = (struct statsd *)arg;
	struct statistic	*stat;
	struct chain		*chain;
	int			 i;
#endif

	log_info("exiting on signal %d", sig);

#if 0
	for (stat = TAILQ_FIRST(&env->stats); stat;
	    stat = TAILQ_NEXT(stat, entry))
		log_debug("Metric %s = %Lf", stat->metric, stat->value.count);

	for (i = 0; i < STATSD_HASH_SIZE; i++)
		if (!TAILQ_EMPTY(&env->chains[i])) {
			log_debug("env->chains[%d]", i);
			for (chain = TAILQ_FIRST(&env->chains[i]); chain;
			    chain = TAILQ_NEXT(chain, entry))
				log_debug("> %s", chain->stat->metric);
		}
#endif

	exit(0);
}

int
main(int argc, char *argv[])
{
	int			 c;
	int			 debug = 0;
	int			 noaction = 0;
	const char		*conffile = STATSD_CONF_FILE;
	struct event_config	*cfg;
	struct statsd		*env;
	int			 i;
	struct event		*sig_hup, *sig_int, *sig_term;
	char			*path;
	struct listen_addr	*la;

	log_init(1);	/* log to stderr until daemonized */

	while ((c = getopt(argc, argv, "df:nv")) != -1) {
		switch (c) {
		case 'd':
			debug = 1;
			break;
		case 'f':
			conffile = optarg;
			break;
		case 'n':
			noaction++;
			break;
		case 'v':
			//flags |= ENQUEUE_F_VERBOSE;
			break;
		default:
			usage();
			/* NOTREACHED */
		}
	}

	argc -= optind;
	argv += optind;
	if (argc > 0)
		usage();

	if ((env = parse_config(conffile, 0)) == NULL)
		exit(1);

	if (noaction) {
		fprintf(stderr, "configuration ok\n");
		exit(0);
	}

#if 0
	if (geteuid())
		errx(1, "need root privileges");

	if ((pw = getpwnam(ENQUEUE_USER)) == NULL)
		errx(1, "unknown user %s", ENQUEUE_USER);
#endif

	log_init(debug);

	if (!debug) {
		if (daemon(1, 0) == -1)
			err(1, "failed to daemonize");
	}

	log_init(1);

	if ((cfg = event_config_new()) == NULL)
		fatalx("event_config_new");

#ifdef __APPLE__
	/* Don't use kqueue(2) on OS X */
	event_config_avoid_method(cfg, "kqueue");
#endif

	env->base = event_base_new_with_config(cfg);
	if (!env->base)
		fatalx("event_base_new_with_config");
	event_config_free(cfg);

	signal(SIGPIPE, SIG_IGN);
	sig_hup = evsignal_new(env->base, SIGHUP, handle_signal, env);
	sig_int = evsignal_new(env->base, SIGINT, handle_signal, env);
	sig_term = evsignal_new(env->base, SIGTERM, handle_signal, env);
	evsignal_add(sig_hup, NULL);
	evsignal_add(sig_int, NULL);
	evsignal_add(sig_term, NULL);

	for (la = TAILQ_FIRST(&env->listen_addrs); la; ) {
		switch (la->sa.ss_family) {
		case AF_INET:
			((struct sockaddr_in *)&la->sa)->sin_port =
			    htons(la->port);
			break;
		case AF_INET6:
			((struct sockaddr_in6 *)&la->sa)->sin6_port =
			    htons(la->port);
			break;
		default:
			fatalx("");
		}

		log_info("listening on %s:%hu",
		    log_sockaddr((struct sockaddr *)&la->sa), la->port);

		if ((la->fd = socket(la->sa.ss_family, SOCK_DGRAM, 0)) == -1)
			fatal("socket");

		if (fcntl(la->fd, F_SETFL, O_NONBLOCK) == -1)
			fatal("fcntl");

		if (bind(la->fd, (struct sockaddr *)&la->sa,
		    SA_LEN((struct sockaddr *)&la->sa)) == -1) {
			struct listen_addr	*nla;

			log_warn("bind on %s failed, skipping",
			    log_sockaddr((struct sockaddr *)&la->sa));
			close(la->fd);
			nla = TAILQ_NEXT(la, entry);
			TAILQ_REMOVE(&env->listen_addrs, la, entry);
			free(la);
			la = nla;
			continue;
		}

		la = TAILQ_NEXT(la, entry);
	}

	/* HTTP server */
	if ((env->httpd = evhttp_new(env->base)) == NULL)
		fatalx("evhttp_new");
	if (evhttp_bind_socket(env->httpd, "0.0.0.0",
	    STATSD_DEFAULT_HTTP_PORT) != 0)
		fatalx("evhttp_bind_socket");
	/* Only care about GET & DELETE methods */
	evhttp_set_allowed_methods(env->httpd,
	    EVHTTP_REQ_GET|EVHTTP_REQ_DELETE);
	for (i = 0; i < STATSD_MAX_TYPE; i++) {
		if ((path = calloc(strlen(dispatch[i].path) + 2,
		    sizeof(char))) == NULL)
			fatal("calloc");
		sprintf(path, "/%s", dispatch[i].path);
		evhttp_set_cb(env->httpd, path, dispatch[i].list_cb,
		    (void *)env);
		free(path);
	}

	if (graphite_init(env->base) < 0)
		fatalx("graphite_init");
	if ((env->graphite_conn = graphite_connection_new(env->graphite_host,
	    env->graphite_port, env->graphite_reconnect)) == NULL)
		fatalx("graphite_connection_new");
	graphite_connection_setcb(env->graphite_conn, graphite_connect_cb,
	    graphite_disconnect_cb, (void *)env);
	env->graphite_ev = event_new(env->base, -1, EV_PERSIST,
	    graphite_timer_cb, (void *)env);
	evtimer_add(env->graphite_ev, &env->graphite_interval);
	if ((env->stats_conn = graphite_connection_new(env->stats_host,
	    env->stats_port, env->stats_reconnect)) == NULL)
		fatalx("graphite_connection_new");
	graphite_connection_setcb(env->stats_conn, stats_connect_cb,
	    stats_disconnect_cb, (void *)env);
	env->stats_ev = event_new(env->base, -1, EV_PERSIST, stats_timer_cb,
	    (void *)env);

	log_info("startup");

	for (la = TAILQ_FIRST(&env->listen_addrs); la; ) {
		la->ev = event_new(env->base, la->fd, EV_READ|EV_PERSIST,
		    statsd_read_cb, (void *)env);
		event_add(la->ev, NULL);
		la = TAILQ_NEXT(la, entry);
	}

	graphite_connect(env->graphite_conn);
	graphite_connect(env->stats_conn);

	event_base_dispatch(env->base);

	return (0);
}
