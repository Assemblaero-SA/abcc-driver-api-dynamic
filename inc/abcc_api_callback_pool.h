/*******************************************************************************
** ABCC API — ADI callback thread pool (POSIX)
**
** Offloads pnGetAdiValue/pnSetAdiValue invocations to worker threads so that
** the driver main loop is not stalled by slow application callbacks (e.g.
** Python/ctypes bridges that take GIL or do I/O).
**
** When ABCC_API_CFG_ADI_CALLBACK_POOL_ENABLED == 0, the _Dispatch function
** invokes the callback synchronously on the caller's thread, and Init/Shutdown
** are no-ops. This preserves exact legacy behavior.
**
** Thread-safety:
**    - _Init / _Shutdown: call once, from the application's main thread
**      before any calls to ABCC_API_Run() / after the last call.
**    - _Dispatch: safe to call from any thread.
**
** Ordering guarantees:
**    - With pool size 1, callbacks for a given ADI always execute in the
**      order they were dispatched. With pool size >1, only partial ordering
**      is guaranteed; callbacks for the same ADI may execute on different
**      workers concurrently. Use pool size 1 if your callbacks mutate shared
**      state without their own synchronization.
**
** Backpressure policy:
**    - If the queue is full, _Dispatch falls back to synchronous execution on
**      the caller's thread. This bounds memory use and avoids data loss, at
**      the cost of temporarily blocking the driver loop when sustained
**      callback load exceeds pool throughput. This is strictly safer than
**      blocking indefinitely (which would re-create the ABCC watchdog
**      timeout problem) or silently dropping callbacks (which would corrupt
**      application state).
********************************************************************************
*/
#ifndef ABCC_API_CALLBACK_POOL_H_
#define ABCC_API_CALLBACK_POOL_H_

#include "abcc_types.h"
#include "abcc_application_data_interface.h"

/*
** Master enable/disable. Define to 0 in abcc_api_config.h to revert to the
** original synchronous callback behavior.
*/
#ifndef ABCC_API_CFG_ADI_CALLBACK_POOL_ENABLED
#define ABCC_API_CFG_ADI_CALLBACK_POOL_ENABLED   ( 0 )
#endif

/*
** Number of worker threads. 1 preserves per-ADI ordering; 2+ allows parallel
** callback execution (use only if callbacks are reentrant-safe).
*/
#ifndef ABCC_API_CFG_ADI_CALLBACK_POOL_SIZE
#define ABCC_API_CFG_ADI_CALLBACK_POOL_SIZE      ( 1 )
#endif

/*
** Max pending callbacks in the queue. Must be a power of two for the ring
** buffer logic. Sized for ~1s of worst-case cycle backlog.
*/
#ifndef ABCC_API_CFG_ADI_CALLBACK_POOL_QUEUE
#define ABCC_API_CFG_ADI_CALLBACK_POOL_QUEUE     ( 256 )
#endif

/*
** Sanity check: queue size must be a power of two (for fast modulo).
*/
#if( ( ABCC_API_CFG_ADI_CALLBACK_POOL_QUEUE                                    \
     & ( ABCC_API_CFG_ADI_CALLBACK_POOL_QUEUE - 1 ) ) != 0 )
#error "ABCC_API_CFG_ADI_CALLBACK_POOL_QUEUE must be a power of two"
#endif

/*
** Callback kind — determines which function pointer on the AD_AdiEntry is
** invoked on the worker thread.
*/
typedef enum
{
   ABCC_API_CB_KIND_GET = 0,
   ABCC_API_CB_KIND_SET = 1
}
ABCC_API_CallbackKind;

/*------------------------------------------------------------------------------
** Initialize the pool. Idempotent; safe to call multiple times. Returns
** TRUE on success, FALSE on failure (e.g. pthread_create failure).
** No-op (returns TRUE) when ABCC_API_CFG_ADI_CALLBACK_POOL_ENABLED == 0.
**------------------------------------------------------------------------------
*/
EXTFUNC BOOL ABCC_API_CallbackPoolInit( void );

/*------------------------------------------------------------------------------
** Drain the queue and stop all worker threads. Blocks until in-flight work
** is done. Safe to call if _Init was never called or already shutdown.
** No-op when ABCC_API_CFG_ADI_CALLBACK_POOL_ENABLED == 0.
**------------------------------------------------------------------------------
*/
EXTFUNC void ABCC_API_CallbackPoolShutdown( void );

/*------------------------------------------------------------------------------
** Dispatch an ADI callback.
**
** When ABCC_API_CFG_ADI_CALLBACK_POOL_ENABLED == 1:
**    - Enqueues the invocation and returns immediately.
**    - If the queue is full OR the pool is not running, falls back to
**      synchronous execution (same as disabled mode).
**
** When ABCC_API_CFG_ADI_CALLBACK_POOL_ENABLED == 0:
**    - Invokes the callback synchronously on the caller's thread.
**
** Arguments mirror the ABCC_GetAdiValueFuncType / ABCC_SetAdiValueFuncType
** signatures exactly.
**------------------------------------------------------------------------------
*/
EXTFUNC void ABCC_API_CallbackPoolDispatch(
      ABCC_API_CallbackKind       eKind,
      const AD_AdiEntryType*      psAdiEntry,
      UINT8                       bNumElements,
      UINT8                       bStartIndex );

#endif  /* ABCC_API_CALLBACK_POOL_H_ */