/*
 * A simple implementation of something like SIBENCH as described in 
 * Michael Cahill's thesis[1].
 *
 * Generate a mixture of select-the-whole table and update-one-random-row
 * queries from some number of threads.
 *
 * Thomas Munro
 *
 * [1] https://ses.library.usyd.edu.au/bitstream/2123/5353/1/michael-cahill-2009-thesis.pdf
 */

#include "libpq-fe.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

struct thread_context
{
	pthread_t thread_handle;
	const char *conn_info;
	struct timeval finish_time;
	int queries_per_update;
	int rows;
	bool ssi;
	int thread_number;
	int transactions;
	int failures;
	int cycle;
};

static void *
thread_main(void *arg)
{
	struct timeval now;
	struct thread_context *context = arg;
	PGconn *conn;
	PGresult *result;
	int i;
	unsigned seed = context->thread_number;

	conn = PQconnectdb(context->conn_info);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "thread %d failed to connect\n", context->thread_number);
		return NULL;
	}

	result = PQexec(conn,
					context->ssi ? "set default_transaction_isolation to serializable"
								 : "set default_transaction_isolation to \"repeatable read\"");
	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "thread %d failed to set isolation level\n", context->thread_number);
		goto fail;
	}
	PQclear(result);

	/* Start each thread at a different phase. */
	i = context->thread_number;
	for (;;)
	{
		/* Generate mix of update and select. */
		if (i++ % context->cycle == 0)
		{
			char buffer[80];

			snprintf(buffer,
					 sizeof(buffer),
					 "update sibench set i = i where i = %d",
					 rand_r(&seed) % context->rows);
			result = PQexec(conn, buffer);
			if (PQresultStatus(result) != PGRES_COMMAND_OK)
				++context->failures;
			++context->transactions;
			PQclear(result);
		}
		else
		{
			result = PQexec(conn, "select * from sibench");
			if (PQresultStatus(result) != PGRES_TUPLES_OK)
				++context->failures;
			++context->transactions;
			PQclear(result);
		}
		gettimeofday(&now, NULL);
		if (now.tv_sec > context->finish_time.tv_sec ||
			(now.tv_sec == context->finish_time.tv_sec &&
			 now.tv_usec >= context->finish_time.tv_usec))
			break;
	}
	
fail:
	PQfinish(conn);
	return NULL;
}

int
main(int argc, char *argv[])
{
	struct timeval finish_time;
	struct thread_context *thread_contexts;
	const char *conn_info;
	int queries_per_update;
	int rows;
	int seconds;
	bool ssi;
	char buffer[256];
	PGconn *conn;
	PGresult *result;
	int threads;
	int total_transactions;
	int total_failures;
	int i;

	/* Defaults. */
	conn_info = "dbname=postgres";
	queries_per_update = 1;
	rows = 10;
	seconds = 60;
	ssi = false;
	threads = 2;

	/* Scan arguments. */
	for (i = 1; i < argc; ++i)
	{
		const char *arg = argv[i];
		bool more = (i + 1) < argc;

		if (strcmp(arg, "--conn-info") == 0 && more)
			conn_info = argv[++i];
		else if (strcmp(arg, "--queries-per-update") == 0 && more)
			queries_per_update = atoi(argv[++i]);
		else if (strcmp(arg, "--rows") == 0 && more)
			rows = atoi(argv[++i]);
		else if (strcmp(arg, "--seconds") == 0 && more)
			seconds = atoi(argv[++i]);
		else if (strcmp(arg, "--threads") == 0 && more)
			threads = atoi(argv[++i]);
		else if (strcmp(arg, "--ssi") == 0)
			ssi = true;
		else
		{
			fprintf(stderr, "<help wanted>\n");
			return EXIT_FAILURE;
		}
	}

	/* Compute the finish time. */
	gettimeofday(&finish_time, NULL);
	finish_time.tv_sec += seconds;

	/* Initialize schema. */
	conn = PQconnectdb(conn_info);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Failed to connect.\n");
		return EXIT_FAILURE;
	}
	result = PQexec(conn, "drop table if exists sibench");
	if (PQresultStatus(result) != PGRES_COMMAND_OK)
		goto fail;
	PQclear(result);
	snprintf(buffer,
			 sizeof(buffer),
			 "create table sibench (i int primary key); insert into sibench select generate_series(1, %d); analyze",
			 rows);
	result = PQexec(conn, buffer);
	if (PQresultStatus(result) != PGRES_COMMAND_OK)
		goto fail;
	PQclear(result);

	PQfinish(conn);

	/* Prepare thread contexts and launch. */
	thread_contexts = malloc(sizeof(struct thread_context) * threads);
	if (thread_contexts == NULL)
	{
		fprintf(stderr, "out of memory\n");
		return EXIT_FAILURE;
	}
	for (i = 0; i < threads; ++i)
	{
		pthread_attr_t thread_attr;

		pthread_attr_init(&thread_attr);
		thread_contexts[i].conn_info = conn_info;
		thread_contexts[i].finish_time = finish_time;
		thread_contexts[i].queries_per_update = queries_per_update;
		thread_contexts[i].ssi = ssi;
		thread_contexts[i].thread_number = i;
		thread_contexts[i].rows = rows;
		thread_contexts[i].transactions = 0;
		thread_contexts[i].failures = 0;
		thread_contexts[i].cycle = queries_per_update + 1;
		if (pthread_create(&thread_contexts[i].thread_handle,
						   &thread_attr,
						   thread_main,
						   &thread_contexts[i]) < 0)
		{
			perror("pthread_create");
			return EXIT_FAILURE;
		}
	}
	/* Wait for them to finish. */
	for (i = 0; i < threads; ++i)
	{
		if (pthread_join(thread_contexts[i].thread_handle, NULL) < 0)
		{
			perror("pthread_join");
			return EXIT_FAILURE;
		}
	}
	/* Count up the total tps. */
	total_transactions = 0;
	total_failures = 0;
	for (i = 0; i < threads; ++i)
	{
		total_transactions += thread_contexts[i].transactions;
		total_failures += thread_contexts[i].failures;
	}
	printf("TPS = %f, failures = %d\n",
		   (double) total_transactions / (double) seconds,
           total_failures);

	return EXIT_SUCCESS;

fail:
	fprintf(stderr, "Failed: %s\n", PQerrorMessage(conn));
	PQfinish(conn);

	return EXIT_FAILURE;
}


