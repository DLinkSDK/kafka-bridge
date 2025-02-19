#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <errno.h>
#include <librdkafka/rdkafka.h>
#include <pthread.h>
#include "tcp2kafka.h"

#define MAX_POOL 40

extern rd_kafka_t *new_rk(int partition);
extern void close_rk(rd_kafka_t *rk);
extern rd_kafka_topic_t *new_topic(rd_kafka_t *rk, char *topic_name);
extern void close_topic(rd_kafka_topic_t *topic_name);

extern int run;
extern char topic_name[256];

pthread_mutex_t mutex;

struct st_rk_pool
{
	int        status;
	rd_kafka_t *rk;
	time_t     ts;
};

struct st_rk_pool rk_pool[MAX_POOL];

void init_rk_pool()
{
	pthread_mutex_lock(&mutex);
	int i = 0;
	for(i = 0; i < MAX_POOL; i++)
	{
		rk_pool[i].status = -1;
		rk_pool[i].rk = NULL;
		rk_pool[i].ts = 0;
	}
	pthread_mutex_unlock(&mutex);
}

rd_kafka_t *get_rk(int i)
{
	if(i >= MAX_POOL)
		return 0;

	pthread_mutex_lock(&mutex);
	if (rk_pool[i].status == 0)
	{
		rk_pool[i].status = 1;
		time(&rk_pool[i].ts);
		pthread_mutex_unlock(&mutex);
		//printf("found rk in pool, num:[%d]\n", i);
		return rk_pool[i].rk;
	}
	else if (rk_pool[i].status == -1)
	{
		rd_kafka_t *rk = new_rk(i);
		if (rk)
		{
			rk_pool[i].status = 1;
			rk_pool[i].rk = rk;
			time(&rk_pool[i].ts);
			pthread_mutex_unlock(&mutex);
			return rk;
		}
	}
	else
	{
		ACCESS_PRINT("rk not found.\n");
	}

	pthread_mutex_unlock(&mutex);
	return 0;
}

void keeplive_rk(int i)
{
	pthread_mutex_lock(&mutex);
	rk_pool[i].status = 0;
	time(&rk_pool[i].ts);
	pthread_mutex_unlock(&mutex);
}

void clean_rk(int i)
{
	pthread_mutex_lock(&mutex);
	rk_pool[i].status = -1;
	close_rk(rk_pool[i].rk);
	rk_pool[i].rk = NULL;
	pthread_mutex_unlock(&mutex);
}

int tcp_write(int fd,void *buffer,int length)
{
	int bytes_left;
	int written_bytes;
	char *ptr;

	ptr = buffer;
	bytes_left = length;
	while (bytes_left > 0)
	{

		written_bytes = write(fd, ptr, bytes_left);
		if (written_bytes <= 0)
		{
			if(errno == EINTR)
				written_bytes = 0;
			else
				return -1;
		}
		bytes_left -= written_bytes;
		ptr += written_bytes;
	}
	return 0;
}

int tcp_read(int sockfd, void *pbuf, size_t len, int time_out)
{
	size_t              ileft, iread;
	unsigned char       *ptr;
	int                 rc, flag;
	fd_set       rset;
	struct timeval      timeout;
	struct timeval      *ptimeout;
	time_t               begin_time,end_time;

	if (time_out == -1)
	{
		ptimeout=NULL;
	}
	else
	{
        ptimeout = &timeout;
    }

    timeout.tv_sec = time_out;
    timeout.tv_usec = 0;

    ptr = pbuf;
    ileft = len;
    flag = 0;
    time(&begin_time);

    while( ileft > 0 )
    {
        FD_ZERO(&rset);
        FD_SET(sockfd,&rset);
        time(&end_time);
        if ( flag == 1 )
            timeout.tv_sec = time_out - end_time + begin_time;
        else
            timeout.tv_sec = time_out;
        flag = 1;
        rc = select(sockfd+1,&rset,NULL,NULL,ptimeout);
        if((rc == 1) && (FD_ISSET( sockfd,&rset )))
        {
            iread = read(sockfd, ptr, ileft);
            if(iread <= 0)
            {
                if( errno==EINTR ) continue;
                return(-1);
            }
            ileft -= iread;
            ptr += iread;
        }else if (rc == 0 )
        {
            return(-2);
        }else if (rc < 0)
        {
            if( errno==EINTR )  continue;
            return(-3);
        }
    }
    return(len);
}


void worker(void *arg)
{
	int new_fd = *(int *)arg;
	free(arg);

	char tmp[32] = { 0 };
	int i = 0, rc = 0, len = 0;

	struct linger slLinger;
	slLinger.l_onoff  = 1;
	slLinger.l_linger = 1;

	rc = setsockopt(new_fd, SOL_SOCKET, SO_LINGER, &slLinger, sizeof(struct linger));
	if (rc == -1)
	{
		ACCESS_PRINT("setsockopt() error...\n");
		close(new_fd);
		return;
	}

	rc = tcp_read(new_fd, tmp, 4, 80);
	if (rc <= 0)
	{
		ACCESS_PRINT("tcp_read(4) error...\n");
		close(new_fd);
		return;
	}

	len = atoi(tmp);
	if (len < 2 || len > 31)
	{
		ACCESS_PRINT("header len[%d] too long ...\n", len);
		close(new_fd);
		return;
	}

	int partition = -1;
	unsigned long ts = 0;

	rc = tcp_read(new_fd, tmp, len, 10);
        if (rc <= 0)
        {
                ACCESS_PRINT("tcp_read(header) error...\n");
                close(new_fd);
                return;
        }

        char *p = tmp, *sep = NULL;
        int n = 0;

	//printf("len:[%d] tmp:[%s]\n", len, tmp);
        for (i = 0; i < len; i++)
        {
                sep = strchr(p, '|');
                if (sep)
                        *sep = 0x00;

                if (n == 0)
                        partition = atoi(p);
                else if (n == 1)
                        ts = (unsigned long)atol(p);

                if (sep)
                        p = sep + 1;
                else
                        break;
                n++;
        }

	if (partition < 0)
	{
		close(new_fd);
		return;
	}

	rd_kafka_t *rk = get_rk(partition);
	if (rk == NULL)
	{
		ACCESS_PRINT("create kafka rk.partition[%d] err!\n", partition);
		goto err_end;
	}

	rd_kafka_topic_t * topic = new_topic(rk, topic_name);
	if (!topic)
	{
		ACCESS_PRINT("create topic:[%s] err!\n");
		clean_rk(partition);
		goto err_end;
	}

	int sum = 0;
	char *buf = NULL, *key = NULL;
	long buf_len = 0, key_len = 0, val_len = 0;
	//printf("begin...p:[%d]\n", partition);

	while (run)
	{
		memset(tmp, 0x00, sizeof(tmp));
		rc = tcp_read(new_fd, tmp, 8, 10);
		if (rc <= 0)
		{
			ACCESS_PRINT("tcp_read(8) error...\n");
			goto end;
		}

		//printf("rc:[%d] tmp:[%s]\n", rc, tmp);

		buf_len = strtol(tmp, NULL, 16);
		if (buf_len <= 8)
			break;

		buf = (char *)malloc(buf_len + 1);
		memset(buf, 0x00, buf_len + 1);

		rc = tcp_read(new_fd, buf, buf_len, 60);
		if (rc <= 0)
		{
			ACCESS_PRINT("tcp_read(data) error...\n");
			free(buf);
			goto end;
		}
		memcpy(tmp, buf, 8);
		tmp[8] = 0x00;
		key_len = strtol(tmp, NULL, 16);

		if (key_len == 0)
			key = NULL;
		else
			key = buf + 8;

		val_len = buf_len - key_len - 8;

		//printf("1...[%.20s]\n", (void *)(buf + 8 + key_len));
		if (rd_kafka_produce(topic, partition, RD_KAFKA_MSG_F_COPY,
					(void *)(buf + 8 + key_len), val_len,
					key, key_len, NULL) == -1) {
			ERR_PRINT("Failed to produce[%d] to topic %s: %s\n", partition, rd_kafka_topic_name(topic), rd_kafka_err2str(rd_kafka_last_error()));
			ERR_PRINT("msg len:[%d]\n", val_len);
			free(buf);
			goto end;
		}
		sum++;
		free(buf);
	}

	close_topic(topic);
	keeplive_rk(partition);

	ACCESS_PRINT("topic_name=[%s] [%lu]-P[%d] sum=[%d]\n",topic_name, ts, partition, sum);

	tcp_write(new_fd, "00000000", 8);

	close(new_fd);
	return;

end:
	close_topic(topic);
	keeplive_rk(partition);

err_end:
	tcp_write(new_fd, "00000001", 8);
	ACCESS_PRINT("topic_name=[%s] [%lu]-P[%d] sum=[%d]\n",topic_name, ts, partition, sum);
	close(new_fd);
}

void monitor()
{
	int i = 0;
	time_t now_ts;
	while(run)
	{
		time(&now_ts);

		pthread_mutex_lock(&mutex);

		for (i = 0; i < MAX_POOL; i++)
		{
			if (rk_pool[i].status == 0 && rk_pool[i].ts + 289 < now_ts)
			{
				rk_pool[i].status = -1;
				close_rk(rk_pool[i].rk);
				rk_pool[i].rk = NULL;
				ACCESS_PRINT("pool timeout process end, num:[%d]\n", i);
			}
		}
		pthread_mutex_unlock(&mutex);
		sleep(5);
	}
}

void clean_conn()
{
	int i = 0, n = 0;
	while(run)
	{
		pthread_mutex_lock(&mutex);
		for (i = 0; i < MAX_POOL; i++)
		{
			if (rk_pool[i].status == 0)
			{
				rk_pool[i].status = -2;
				close_rk(rk_pool[i].rk);
				ACCESS_PRINT("clean [%d]...\n", i);
				n++;
			}else if( rk_pool[i].status == -1)
			{
				rk_pool[i].status = -2;
				ACCESS_PRINT("clean [%d]...\n", i);
				n++;
			}
		}
		if (n >= MAX_POOL)
		{
			ACCESS_PRINT("clean ok!\n");
			break;
		}
		sleep(1);
	}
}
