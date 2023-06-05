#include "postgres.h"
#include "fmgr.h"
#include "storage/lwlock.h"
#include "storage/lwlocknames.h"
#include "storage/procarray.h"
#include "storage/lmgr.h"
#include "replication/walsender_private.h"

PG_MODULE_MAGIC;

/*
PG_FUNCTION_INFO_V1(gp_aquire_transaction_start_lock);
PG_FUNCTION_INFO_V1(gp_release_transaction_start_lock);
PG_FUNCTION_INFO_V1(gp_wait_running_transaction_end_except_one);
*/
PG_FUNCTION_INFO_V1(get_snapshot_content);
PG_FUNCTION_INFO_V1(deliver_snapshot);

/*
Datum
gp_aquire_transaction_start_lock(PG_FUNCTION_ARGS)
{
	LWLockAcquire(StartTransactionLock, LW_EXCLUSIVE);
}

Datum
gp_release_transaction_start_lock(PG_FUNCTION_ARGS)
{
	LWLockRelease(StartTransactionLock);
}

Datum
gp_wait_running_transaction_end_except_one(PG_FUNCTION_ARGS)
{
	DistributedTransactionId not_wait = (DistributedTransactionId)PG_GETARG_UINT64(0);
	List* gxids = ListAllGxid();
	ListCell* cell;
	foreach(cell, gxids)
	{
		DistributedTransactionId gxid = *((DistributedTransactionId*)lfirst(cell));
		if(gxid == not_wait)
		{
			continue;
		}
		GxactLockTableWait(gxid);
	}
}
*/

unsigned long long parse(char* p, int len)
{
	char* buf = malloc(len+1);
	memcpy(buf, p, len);
	buf[len] = 0;
	unsigned long long ret = 0;
	sscanf(buf, "%llu", &ret);
	return ret;
}

//根据传入的快照id返回快照内容
Datum get_snapshot_content(PG_FUNCTION_ARGS)
{
	//FILE* f = fopen("/home/gpadmin/wangchonglog", "a");

	WalSnd	   *cdc_walsnd = NULL;
	//fprintf(f, "%d\n", max_wal_senders);
	for (int i = 0; i < max_wal_senders; i++)
	{
		WalSnd	   *walsnd = &WalSndCtl->walsnds[i];
		if(walsnd->is_for_cdc)
		{
			cdc_walsnd = walsnd;
			//fprintf(f, "found cdc walsender:%d\n", cdc_walsnd->pid);
			break;
		}
	}

	char snapshot_buf[200];//看下pg有没有对快照的体积做限制？

	char* snapshot_id = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char snapshot_file_name[200];
	char* dir = "/home/gpadmin/data/gpdb7_data/master/gpseg-1/pg_snapshots/";
	strcpy(snapshot_file_name, dir);
	strcpy(snapshot_file_name + strlen(dir), snapshot_id);
	snapshot_file_name[strlen(dir) + strlen(dir)] = 0;
	//fprintf(f, "%s\n", snapshot_file_name);

	FILE* f_snapshot = fopen(snapshot_file_name, "r");
	int read_cnt = fread(snapshot_buf, 1, 200, f_snapshot);
	snapshot_buf[read_cnt] = 0;
	//fprintf(f, "%s\n", snapshot_buf);
	
	//fclose(f);

	PG_RETURN_TEXT_P(cstring_to_text(snapshot_buf));
}

Datum deliver_snapshot(PG_FUNCTION_ARGS)
{
	//FILE* f = fopen("/home/gpadmin/wangchonglog", "a");

	WalSnd	   *cdc_walsnd = NULL;
	//fprintf(f, "%d\n", max_wal_senders);
	for (int i = 0; i < max_wal_senders; i++)
	{
		WalSnd	   *walsnd = &WalSndCtl->walsnds[i];
		if(walsnd != NULL && walsnd->is_for_cdc)
		{
			cdc_walsnd = walsnd;
			//fprintf(f, "found cdc walsender:%d\n", cdc_walsnd->pid);
			break;
		}
	}

	char* snapshot_buf = text_to_cstring(PG_GETARG_TEXT_PP(0));

	char *v_start = strstr(snapshot_buf, "dsxmin:");
	v_start += 7;
	char *v_end = strstr(v_start, "\n");

	unsigned long long parse_ret = parse(v_start, v_end - v_start);
	//fprintf(f, "xmin:%llu", parse_ret);
	cdc_walsnd->xmin = (DistributedTransactionId)parse_ret;

	

	v_start = strstr(snapshot_buf, "dsxmax");
	v_start += 7;
	v_end = strstr(v_start, "\n");
 	parse_ret = parse(v_start, v_end - v_start);
	//fprintf(f, "xmax:%llu", parse_ret);
	cdc_walsnd->xmax = (DistributedTransactionId)parse_ret;

	int xip_cnt = 0;
	v_start = strstr(snapshot_buf, "dsxcnt");
	v_start += 7;
	v_end = strstr(v_start, "\n");
 	xip_cnt = (int)parse(v_start, v_end - v_start);
	//fprintf(f, "xip_cnt:%d", xip_cnt);
	cdc_walsnd->cnt_xips = xip_cnt;
	cdc_walsnd->xips = malloc(xip_cnt * sizeof(DistributedTransactionId));

	char* p = snapshot_buf;
	for(int i = 0; i < xip_cnt; ++i)
	{
		v_start = strstr(p, "dsxip");
		v_start += 6;
		v_end = strstr(v_start, "\n");
 		parse_ret = parse(v_start, v_end - v_start);
		//fprintf(f, "xip:%llu", parse_ret);

		cdc_walsnd->xips[i] = (DistributedTransactionId)parse_ret;

		p = v_end;
	}
	
	cdc_walsnd->snapshot_ready = true;
	//fclose(f);
}

/*
Datum deliver_snapshot(PG_FUNCTION_ARGS)
{
	FILE* f = fopen("/home/gpadmin/wangchonglog", "a");

	WalSnd	   *cdc_walsnd = NULL;
	fprintf(f, "%d\n", max_wal_senders);
	for (int i = 0; i < max_wal_senders; i++)
	{
		WalSnd	   *walsnd = &WalSndCtl->walsnds[i];
		if(walsnd->is_for_cdc)
		{
			cdc_walsnd = walsnd;
			fprintf(f, "found cdc walsender:%d\n", cdc_walsnd->pid);
			break;
		}
	}

	char snapshot_buf[200];

	char* snapshot_id = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char snapshot_file_name[200];
	char* dir = "/home/gpadmin/data/gpdb7_data/master/gpseg-1/pg_snapshots/";
	strcpy(snapshot_file_name, dir);
	strcpy(snapshot_file_name + strlen(dir), snapshot_id);
	snapshot_file_name[strlen(dir) + strlen(dir)] = 0;
	fprintf(f, "%s\n", snapshot_file_name);

	FILE* f_snapshot = fopen(snapshot_file_name, "r");
	int read_cnt = fread(snapshot_buf, 1, 200, f_snapshot);
	snapshot_buf[read_cnt] = 0;
	fprintf(f, "%s\n", snapshot_buf);
	
	char *v_start = strstr(snapshot_buf, "dsxmin:");
	v_start += 7;
	char *v_end = strstr(v_start, "\n");

	unsigned long long parse_ret = parse(v_start, v_end - v_start);
	fprintf(f, "xmin:%llu", parse_ret);
	cdc_walsnd->xmin = (DistributedTransactionId)parse_ret;

	v_start = strstr(snapshot_buf, "dsxmax");
	v_start += 7;
	v_end = strstr(v_start, "\n");
 	parse_ret = parse(v_start, v_end - v_start);
	fprintf(f, "xmax:%llu", parse_ret);
	cdc_walsnd->xmax = (DistributedTransactionId)parse_ret;

	int xip_cnt = 0;
	v_start = strstr(snapshot_buf, "dsxcnt");
	v_start += 7;
	v_end = strstr(v_start, "\n");
 	xip_cnt = (int)parse(v_start, v_end - v_start);
	fprintf(f, "xip_cnt:%d", xip_cnt);
	cdc_walsnd->cnt_xids = xip_cnt;
	cdc_walsnd->xids = malloc(xip_cnt * sizeof(DistributedTransactionId));

	char* p = snapshot_buf;
	for(int i = 0; i < xip_cnt; ++i)
	{
		v_start = strstr(p, "dsxip");
		v_start += 6;
		v_end = strstr(v_start, "\n");
 		parse_ret = parse(v_start, v_end - v_start);
		fprintf(f, "xip:%llu", parse_ret);

		cdc_walsnd->xids[i] = (DistributedTransactionId)parse_ret;

		p = v_end;
	}
	
	cdc_walsnd->snapshot_ready = true;
	fclose(f);
}
*/

/*
Datum deliver_snapshot(PG_FUNCTION_ARGS)
{
	FILE* f = fopen("/home/gpadmin/wangchonglog", "a");

	WalSnd	   *cdc_walsnd = NULL;
	for (i = 0; i < max_wal_senders; i++)
	{
		WalSnd	   *walsnd = &WalSndCtl->walsnds[i];
		if(walsnd->is_for_cdc)
		{
			cdc_walsnd = walsnd;
			fprintf(f, "find cdc walsender:%d\n", cdc_walsnd->Pid);
			break;
		}
	}

	char	   *data = text_to_cstring(PG_GETARG_TEXT_PP(0));
	DistributedTransactionId xmax = *((DistributedTransactionId*)data);
	cdc_walsnd->xmax = xmax;
	fprintf(f, "xmax:%lld\n", cdc_walsnd->xmax);

	data += sizeof(DistributedTransactionId);
	uint64_t cnt = *((uint64_t*)data);
	cdc_walsnd->xids = malloc(cnt * sizeof(DistributedTransactionId));
	
	DistributedTransactionId xmin = xmax;
	for(uint64_t i = 0; i < cnt; ++i)
	{
		data += sizeof(DistributedTransactionId);
		DistributedTransactionId xid = *((DistributedTransactionId*)data);
		cdc_walsnd->xids[i] = xid;
		fprintf(f, "running tx:%lld\n", cdc_walsnd->xids[i]);

		if(xid < xmin)xmin = xid;
	}
	fprintf(f, "xmin:%lld\n", cdc_walsnd->xmin);

	cdc_walsnd->snapshot_ready = true;
	flose(f);
}
*/