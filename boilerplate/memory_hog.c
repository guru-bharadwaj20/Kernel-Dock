#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Parse a positive size in megabytes, falling back on invalid input. */
static size_t parse_size_mb(const char *arg, size_t fallback)
{
    char *end = NULL;
    unsigned long value = strtoul(arg, &end, 10);

    if (!arg || *arg == '\0' || (end && *end != '\0') || value == 0)
        return fallback;
    return (size_t)value;
}

/* Parse sleep duration in milliseconds and convert it to microseconds. */
static useconds_t parse_sleep_ms(const char *arg, useconds_t fallback)
{
    char *end = NULL;
    unsigned long value = strtoul(arg, &end, 10);

    if (!arg || *arg == '\0' || (end && *end != '\0'))
        return fallback;
    return (useconds_t)(value * 1000U);
}

/* Allocate memory chunks in a loop to simulate sustained memory pressure. */
int main(int argc, char *argv[])
{
    const size_t chunk_mb = (argc > 1) ? parse_size_mb(argv[1], 8) : 8;
    const useconds_t sleep_us = (argc > 2) ? parse_sleep_ms(argv[2], 1000U) : 1000U * 1000U;
    const size_t chunk_bytes = chunk_mb * 1024U * 1024U;
    int count = 0;

    while (1) {
        char *mem = malloc(chunk_bytes);
        if (!mem) {
            printf("malloc failed after %d allocations\n", count);
            break;
        }

        memset(mem, 'A', chunk_bytes);
        count++;
        printf("allocation=%d chunk=%zuMB total=%zuMB\n",
               count, chunk_mb, (size_t)count * chunk_mb);
        fflush(stdout);
        usleep(sleep_us);
    }

    return 0;
}
