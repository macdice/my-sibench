PG?=/usr

petit-sibench: petit-sibench.c
	$(CC) -Wall -Werror -o petit-sibench petit-sibench.c -I $(PG)/include -L $(PG)/lib -lpq
