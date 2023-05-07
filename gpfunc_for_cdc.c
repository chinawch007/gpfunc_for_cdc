#include "postgres.h"
#include "fmgr.h"
#include "storage/lwlock.h"
#include "storage/lwlocknames.h"
#include "storage/procarray.h"
#include "storage/lmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gp_aquire_transaction_start_lock);
PG_FUNCTION_INFO_V1(gp_release_transaction_start_lock);
PG_FUNCTION_INFO_V1(gp_wait_running_transaction_end_except_one);

Datum
gp_aquire_transaction_start_lock(PG_FUNCTION_ARGS)
{
	LWLockAcquire(TransactionStartLock, LW_EXCLUSIVE);
}

Datum
gp_release_transaction_start_lock(PG_FUNCTION_ARGS)
{
	LWLockRelease(TransactionStartLock);
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