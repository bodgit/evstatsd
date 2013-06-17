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

struct statistic_dispatch {
	char	 *path;
	void	(*single_cb)(struct evhttp_request *, void *);
	void	(*list_cb)(struct evhttp_request *, void *);
};

__dead void	 usage(void);
int		 statistic_cmp(struct statistic *, struct statistic *);
int		 reading_cmp(struct reading *, struct reading *);
int		 unique_cmp(struct unique *, struct unique *);
void		 stats_timer_cb(int, short, void *);
void		 stats_connect_cb(struct graphite_connection *, void *);
void		 stats_disconnect_cb(struct graphite_connection *, void *);
void		 graphite_connect_cb(struct graphite_connection *, void *);
void		 graphite_disconnect_cb(struct graphite_connection *, void *);
void		 graphite_timer_cb(int, short, void *);
void		 process_generic_list(struct evhttp_request *, void *,
		    enum statistic_type);
void		 process_counter_list(struct evhttp_request *, void *);
void		 process_timer_list(struct evhttp_request *, void *);
void		 process_gauge_list(struct evhttp_request *, void *);
void		 process_set_list(struct evhttp_request *, void *);
void		 process_generic(struct evhttp_request *, void *,
		    enum statistic_type);
void		 process_counter(struct evhttp_request *, void *);
void		 process_timer(struct evhttp_request *, void *);
void		 process_gauge(struct evhttp_request *, void *);
void		 process_set(struct evhttp_request *, void *);
void		 statsd_read_cb(int, short, void *);
void		 handle_signal(int, short, void *);

RB_PROTOTYPE(statistics, statistic, entry, statistic_cmp);
RB_GENERATE(statistics, statistic, entry, statistic_cmp);

RB_PROTOTYPE(readings, reading, entry, reading_cmp);
RB_GENERATE(readings, reading, entry, reading_cmp);

RB_PROTOTYPE(uniques, unique, entry, unique_cmp);
RB_GENERATE(uniques, unique, entry, unique_cmp);

struct statistic_dispatch dispatch[STATSD_MAX_TYPE] = {
	{ "counters", process_counter, process_counter_list },
	{ "timers",   process_timer,   process_timer_list   },
	{ "gauges",   process_gauge,   process_gauge_list   },
	{ "sets",     process_set,     process_set_list     }
};

__dead void
usage(void)
{
	extern char	*__progname;

	fprintf(stderr, "usage: %s [-dnv] [-f file]\n", __progname);
	exit(1);
}

int
statistic_cmp(struct statistic *s1, struct statistic *s2)
{
	return (strcmp(s1->metric, s2->metric));
}

int
reading_cmp(struct reading *r1, struct reading *r2)
{
	if (r1->value > r2->value)
		return (1);
	else if (r1->value < r2->value)
		return (-1);
	else
		return (0);
}

int
unique_cmp(struct unique *u1, struct unique *u2)
{
	return (strcmp(u1->value, u2->value));
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
	graphite_send_metric(env->stats_conn, env->stats_prefix,
	    "bytes.rx", tv, "%lld", env->bytes_rx);
	graphite_send_metric(env->stats_conn, env->stats_prefix,
	    "packets.rx", tv, "%lld", env->packets_rx);
	graphite_send_metric(env->stats_conn, env->stats_prefix,
	    "metrics.rx", tv, "%lld", env->metrics_rx);
	graphite_send_metric(env->stats_conn, env->stats_prefix,
	    "search.mus", tv, "%lld",
	    (env->seek_tv.tv_sec * 1000000) + env->seek_tv.tv_usec);
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
	struct reading		*r1, *r2;
	struct unique		*u1, *u2;
	unsigned long long	 count;
	long double		 sum, min, max, mean;

	gettimeofday(&tv, NULL);

	for (stat = RB_MIN(statistics, &env->stats); stat;
	    stat = RB_NEXT(statistics, &env->stats, stat)) {
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
		case STATSD_TIMER:
			count = sum = min = max = mean = 0;
			if (!RB_EMPTY(&stat->value.timer.readings)) {
				count = stat->value.timer.count;
				r1 = RB_MIN(readings,
				    &stat->value.timer.readings);
				min = max = r1->value;
				while (r1 != NULL) {
					sum += r1->value * r1->count;
					max = MAX(max, r1->value);
					r2 = RB_NEXT(readings,
					    &stat->value.timer.readings, r1);
					RB_REMOVE(readings,
					    &stat->value.timer.readings, r1);
					free(r1);
					r1 = r2;
				}
				mean = sum / count;
			}
#if 0
			/* Necessary? */
			RB_INIT(&stat->value.readings);
#endif
			stat->value.timer.count = 0;
			log_debug("Sending %s.count = %lld to graphite",
			    stat->metric, count);
			log_debug("Sending %s.sum = %Lf to graphite",
			    stat->metric, sum);
			log_debug("Sending %s.upper = %Lf to graphite",
			    stat->metric, max);
			log_debug("Sending %s.lower = %Lf to graphite",
			    stat->metric, min);
			log_debug("Sending %s.mean = %Lf to graphite",
			    stat->metric, mean);
			if (env->state & STATSD_GRAPHITE_CONNECTED) {
				graphite_send_metric(env->graphite_conn,
				    stat->metric, "count", tv, "%lld", count);
				graphite_send_metric(env->graphite_conn,
				    stat->metric, "sum", tv, "%Lf", sum);
				graphite_send_metric(env->graphite_conn,
				    stat->metric, "upper", tv, "%Lf", max);
				graphite_send_metric(env->graphite_conn,
				    stat->metric, "lower", tv, "%Lf", min);
				graphite_send_metric(env->graphite_conn,
				    stat->metric, "mean", tv, "%Lf", mean);
			}
			break;
		case STATSD_SET:
			count = 0;
			u1 = RB_MIN(uniques, &stat->value.uniques);
			while (u1 != NULL) {
				count++;
				u2 = RB_NEXT(uniques, &stat->value.uniques, u1);
				RB_REMOVE(uniques, &stat->value.uniques, u1);
				free(u1->value);
				free(u1);
				u1 = u2;
			}
#if 0
			/* Necessary? */
			RB_INIT(&stat->value.uniques);
#endif
			log_debug("Sending %s.count = %lld to graphite",
			    stat->metric, count);
			if (env->state & STATSD_GRAPHITE_CONNECTED) {
				graphite_send_metric(env->graphite_conn,
				    stat->metric, "count", tv, "%lld", count);
			}
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
		for (stat = RB_MIN(statistics, &env->stats); stat;
		    stat = RB_NEXT(statistics, &env->stats, stat)) {
			/* Not the type we're interested in, next! */
			if (stat->type != type)
				continue;
			evbuffer_add_printf(buf, "\"%s\"", stat->metric);
			if (RB_NEXT(statistics, &env->stats, stat))
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
process_timer_list(struct evhttp_request *req, void *arg)
{
	process_generic_list(req, arg, STATSD_TIMER);
}

void
process_gauge_list(struct evhttp_request *req, void *arg)
{
	process_generic_list(req, arg, STATSD_GAUGE);
}

void
process_set_list(struct evhttp_request *req, void *arg)
{
	process_generic_list(req, arg, STATSD_SET);
}

void
process_generic(struct evhttp_request *req, void *arg,
    enum statistic_type type)
{
	struct statsd		*env = (struct statsd *)arg;
	const char		*metric;
	struct statistic	*stat;
	struct evbuffer		*buf;
	struct statistic	 find;
	struct reading		*r1, *r2;
	struct unique		*u1, *u2;
	int			 i;

	/* Metric is the rest of the URL after "/<type>/" */
	metric = evhttp_uri_get_path(evhttp_request_get_evhttp_uri(req)) +
	    strlen(dispatch[type].path) + 2;

	find.metric = (char *)metric;
	stat = RB_FIND(statistics, &env->stats, &find);

	/* Shouldn't ever happen */
	if (!stat)
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
			    metric, stat->value.count, stat->tv.tv_sec);
			break;
		case STATSD_TIMER:
			evbuffer_add_printf(buf,
			    "{\"name\":\"%s\",\"last_modified\":%lu,\"values\":[",
			    metric, stat->tv.tv_sec);
			for (r1 = RB_MIN(readings, &stat->value.timer.readings); r1;
			    r1 = RB_NEXT(readings, &stat->value.timer.readings, r1)) {
				for (i = 1; i <= r1->count; i++) {
					evbuffer_add_printf(buf, "%Lf",
					    r1->value);
					if (i < r1->count)
						evbuffer_add_printf(buf, ",");
				}
				if (RB_NEXT(readings, &stat->value.timer.readings, r1))
					evbuffer_add_printf(buf, ",");
			}
			evbuffer_add_printf(buf, "]}\n");
			break;
		case STATSD_SET:
			evbuffer_add_printf(buf,
			    "{\"name\":\"%s\",\"last_modified\":%lu,\"values\":[",
			    metric, stat->tv.tv_sec);
			for (u1 = RB_MIN(uniques, &stat->value.uniques); u1;
			    u1 = RB_NEXT(uniques, &stat->value.uniques, u1)) {
				evbuffer_add_printf(buf, "\"%s\"", u1->value);
				if (RB_NEXT(uniques, &stat->value.uniques, u1))
					evbuffer_add_printf(buf, ",");
			}
			evbuffer_add_printf(buf, "]}\n");
			break;
		default:
			/* Shouldn't ever happen, return empty JSON object */
			evbuffer_add_printf(buf, "{}\n");
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

		RB_REMOVE(statistics, &env->stats, stat);

		/* Some statistic types require additional cleanup */
		switch (type) {
		case STATSD_TIMER:
			r1 = RB_MIN(readings, &stat->value.timer.readings);
			while (r1 != NULL) {
				r2 = RB_NEXT(readings,
				    &stat->value.timer.readings, r1);
				RB_REMOVE(readings,
				    &stat->value.timer.readings, r1);
				free(r1);
				r1 = r2;
			}
			break;
		case STATSD_SET:
			u1 = RB_MIN(uniques, &stat->value.uniques);
			while (u1 != NULL) {
				u2 = RB_NEXT(uniques, &stat->value.uniques,
				    u1);
				RB_REMOVE(uniques, &stat->value.uniques, u1);
				free(u1->value);
				free(u1);
				u1 = u2;
			}
			break;
		default:
			break;
		}

		free(stat->metric);
		free(stat);
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
process_timer(struct evhttp_request *req, void *arg)
{
	process_generic(req, arg, STATSD_TIMER);
}

void
process_gauge(struct evhttp_request *req, void *arg)
{
	process_generic(req, arg, STATSD_GAUGE);
}

void
process_set(struct evhttp_request *req, void *arg)
{
	process_generic(req, arg, STATSD_SET);
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
	char			*metric = NULL;
	size_t			 length;
	struct statistic	 find;
	struct statistic	*stat;
	char			*path;
	double			 value, rate;
	enum statistic_type	 type;
	struct timeval		 t0, t1;
	struct reading		*r1, *r2;
	struct unique		*u1, *u2;
	char			*ovalue = NULL;

	bzero(storage, STATSD_MAX_UDP_PACKET);
	slen = sizeof(ss);
	if ((len = recvfrom(fd, storage, sizeof(storage), 0,
	    (struct sockaddr *)&ss, &slen)) < 1)
		return;

	//log_debug("Packet received: \"%s\"", storage);
	env->bytes_rx += len;
	env->packets_rx++;

	ptr = storage;
	while (*ptr != '\0') {
		/* Maybe check fo allowable characters instead? */
		if ((length = strcspn(ptr, ":")) == 0) {
			log_warnx("No metric");
			goto bad;
		}
		metric = calloc(length + 1, sizeof(char));
		strncpy(metric, ptr, length);
		ptr += length;

		if (*ptr != ':') {
			log_warnx("No ':'");
			goto bad;
		}
		ptr++;

		/* Remember where the value starts for checking for +/- later */
		optr = ptr;
		if (((value = strtod(ptr, &nptr)) == 0) && (nptr == ptr)) {
			log_warnx("Bad double at %s", ptr);
			goto bad;
		}

		if (*nptr != '|') {
			log_warnx("No '|'");
			goto bad;
		}

		/* Thanks to the set type, we need the original string value
		 * to track for uniqueness rather than parsed into a double
		 */
		length = nptr - optr;
		ovalue = calloc(length + 1, sizeof(char));
		strncpy(ovalue, optr, length);

		ptr = nptr + 1;

		/* Counter, timer, gauge or set? */
		length = strspn(ptr, "cgms");
		if (!strncmp(ptr, "c", length)) {
			type = STATSD_COUNTER;
		} else if (!strncmp(ptr, "ms", length)) {
			type = STATSD_TIMER;
		} else if (!strncmp(ptr, "g", length)) {
			type = STATSD_GAUGE;
		} else if (!strncmp(ptr, "s", length)) {
			type = STATSD_SET;
		} else {
			log_warnx("Invalid type");
			goto bad;
		}

		ptr += length;

		/* Only counters and timers support sample rates */
		rate = 1;
		if ((type == STATSD_COUNTER || type == STATSD_TIMER) &&
		    *ptr == '|') {
			ptr++;
			if (*ptr != '@') {
				log_warnx("No '@'");
				goto bad;
			}
			ptr++;
			if (((rate = strtod(ptr, &nptr)) == 0) && (nptr == ptr)) {
				log_warnx("Bad double at %s", ptr);
				goto bad;
			}
			ptr = nptr;
		}

		/* Should now be on either a newline or a null */
		if (*ptr == '\n')
			ptr++;

		gettimeofday(&t0, NULL);

		find.metric = metric;
		stat = RB_FIND(statistics, &env->stats, &find);

		gettimeofday(&t1, NULL);

		/* Track how much time we spend searching for metrics */
		timersub(&t1, &t0, &t0);
		timeradd(&env->seek_tv, &t0, &env->seek_tv);

		/* Same metric name, different type */
		if (stat && stat->type != type) {
			log_warnx("Metric %s already exists with different type",
			    metric);
			free(metric);
			free(ovalue);
			metric = ovalue = NULL;
			continue;
		}

		env->metrics_rx++;

		if (!stat) {
			stat = calloc(1, sizeof(struct statistic));
			stat->metric = strdup(metric);
			stat->type = type;

			switch (type) {
			case STATSD_TIMER:
				RB_INIT(&stat->value.timer.readings);
				break;
			case STATSD_SET:
				RB_INIT(&stat->value.uniques);
				break;
			default:
				break;
			}

			RB_INSERT(statistics, &env->stats, stat);

			path = calloc(strlen(dispatch[type].path) + strlen(metric) + 3,
			    sizeof(char));
			sprintf(path, "/%s/%s", dispatch[type].path, metric);
			evhttp_set_cb(env->httpd, path, dispatch[type].single_cb,
			    (void *)env);
			free(path);

			env->count[type]++;
		}

		switch (stat->type) {
		case STATSD_COUNTER:
			stat->value.count += value * (1 / rate);
			break;
		case STATSD_GAUGE:
			if (*optr == '+' || *optr == '-')
				stat->value.count += value;
			else
				stat->value.count = value;
			break;
		case STATSD_TIMER:
			/* Blame pesky median averages for this */
			stat->value.timer.count++;
			r1 = calloc(1, sizeof(struct reading));
			r1->value = value;
			r1->count = 1;
			if ((r2 = RB_INSERT(readings,
			    &stat->value.timer.readings, r1)) != NULL) {
				free(r1);
				r2->count++;
			}
			break;
		case STATSD_SET:
			u1 = calloc(1, sizeof(struct unique));
			/* Use the copy of the original value we made */
			u1->value = ovalue;
			if ((u2 = RB_INSERT(uniques, &stat->value.uniques,
			    u1)) != NULL) {
				log_debug("\"%s\" already in set", value);
				free(u1->value);
				free(u1);
			}
			break;
		default:
			break;
		}

		/* If this wasn't a set metric, we won't have used this
		 * copy of the original string value
		 */
		if (type != STATSD_SET)
			free(ovalue);
		ovalue = NULL;

		/* Record last time this metric was updated */
		gettimeofday(&stat->tv, NULL);

		free(metric);

		continue;
bad:
		if (metric) {
			free(metric);
			metric = NULL;
		}

		if (ovalue) {
			free(ovalue);
			ovalue = NULL;
		}

		/* Try and recover to the next metric in a multi-metric
		 * packet or just fast-forward to the null at the end
		 */
		ptr += strcspn(ptr, "\n");
		if (*ptr == '\n')
			ptr++;
	}
}

void
handle_signal(int sig, short event, void *arg)
{
	log_info("exiting on signal %d", sig);

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
