/*******************************************************************************
** ABCC API — ADI callback thread pool (POSIX implementation).
********************************************************************************
*/
#include "abcc_api_callback_pool.h"
#include "abcc_api_config.h"
#include "abcc_log.h"

#if ABCC_API_CFG_ADI_CALLBACK_POOL_ENABLED

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

/*
** Single queue entry. Small, trivially copyable, no heap ownership.
*/
typedef struct
{
   ABCC_API_CallbackKind   eKind;
   const AD_AdiEntryType*  psAdiEntry;
   UINT8                   bNumElements;
   UINT8                   bStartIndex;
}
pool_Job;

/*
** Ring buffer. Head/tail are indices modulo queue size (power of two).
** Queue is empty iff xHead == xTail; full iff ((xHead + 1) & MASK) == xTail.
** This sacrifices one slot to distinguish empty from full, which is cheaper
** than an explicit count.
*/
#define POOL_QUEUE_SIZE  ( ABCC_API_CFG_ADI_CALLBACK_POOL_QUEUE )
#define POOL_QUEUE_MASK  ( POOL_QUEUE_SIZE - 1 )

static pool_Job          pool_asQueue[ POOL_QUEUE_SIZE ];
static volatile uint32_t pool_xHead;   /* producer writes here, then advances */
static volatile uint32_t pool_xTail;   /* consumer reads here, then advances  */

static pthread_mutex_t   pool_xMutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t    pool_xCondNotEmpty = PTHREAD_COND_INITIALIZER;

static pthread_t         pool_axWorkers[ ABCC_API_CFG_ADI_CALLBACK_POOL_SIZE ];
static BOOL              pool_afWorkerStarted[ ABCC_API_CFG_ADI_CALLBACK_POOL_SIZE ];
static volatile BOOL     pool_fRunning  = FALSE;
static BOOL              pool_fInitDone = FALSE;

/*------------------------------------------------------------------------------
** Invoke the callback described by a job directly, on the current thread.
** Used both by workers and by the synchronous fallback path.
**------------------------------------------------------------------------------
*/
static void pool_InvokeJob( const pool_Job* psJob )
{
   if( psJob->psAdiEntry == NULL )
   {
      return;
   }

   if( psJob->eKind == ABCC_API_CB_KIND_GET )
   {
      if( psJob->psAdiEntry->pnGetAdiValue != NULL )
      {
         psJob->psAdiEntry->pnGetAdiValue( psJob->psAdiEntry,
                                           psJob->bNumElements,
                                           psJob->bStartIndex );
      }
   }
   else  /* ABCC_API_CB_KIND_SET */
   {
      if( psJob->psAdiEntry->pnSetAdiValue != NULL )
      {
         psJob->psAdiEntry->pnSetAdiValue( psJob->psAdiEntry,
                                           psJob->bNumElements,
                                           psJob->bStartIndex );
      }
   }
}

/*------------------------------------------------------------------------------
** Worker thread. Blocks on the condition variable until a job is enqueued
** or shutdown is requested.
**------------------------------------------------------------------------------
*/
static void* pool_WorkerMain( void* pvArg )
{
   (void)pvArg;

   for( ;; )
   {
      pool_Job  sJob;
      BOOL      fHaveJob = FALSE;

      (void)pthread_mutex_lock( &pool_xMutex );

      while( pool_fRunning && ( pool_xHead == pool_xTail ) )
      {
         /*
         ** Queue empty. Wait for producer or shutdown.
         */
         (void)pthread_cond_wait( &pool_xCondNotEmpty, &pool_xMutex );
      }

      if( pool_xHead != pool_xTail )
      {
         sJob = pool_asQueue[ pool_xTail & POOL_QUEUE_MASK ];
         pool_xTail = ( pool_xTail + 1u ) & (uint32_t)(2u * POOL_QUEUE_SIZE - 1u);
         /*
         ** Note: we keep indices in [0, 2*QSIZE) to allow
         ** head==tail==empty, head-tail==QSIZE==full without a count.
         ** Mask with QUEUE_MASK only when indexing the array.
         */
         fHaveJob = TRUE;
      }

      (void)pthread_mutex_unlock( &pool_xMutex );

      if( fHaveJob )
      {
         pool_InvokeJob( &sJob );
         continue;
      }

      /*
      ** Reached only when shutdown was signalled and queue is drained.
      */
      if( !pool_fRunning )
      {
         break;
      }
   }

   return( NULL );
}

/*------------------------------------------------------------------------------
** Try to enqueue a job. Returns TRUE on success, FALSE if the queue is full.
** Caller is expected to fall back to synchronous execution on FALSE.
**------------------------------------------------------------------------------
*/
static BOOL pool_TryEnqueue( const pool_Job* psJob )
{
   BOOL fEnqueued = FALSE;

   (void)pthread_mutex_lock( &pool_xMutex );

   {
      /*
      ** Depth is the raw difference in the 2*QSIZE index space.
      */
      uint32_t xDepth = ( pool_xHead - pool_xTail )
                      & (uint32_t)( 2u * POOL_QUEUE_SIZE - 1u );
      if( xDepth < POOL_QUEUE_SIZE )
      {
         pool_asQueue[ pool_xHead & POOL_QUEUE_MASK ] = *psJob;
         pool_xHead = ( pool_xHead + 1u )
                    & (uint32_t)( 2u * POOL_QUEUE_SIZE - 1u );
         fEnqueued = TRUE;
      }
   }

   (void)pthread_mutex_unlock( &pool_xMutex );

   if( fEnqueued )
   {
      /*
      ** Wake one waiting worker. pthread_cond_signal is cheap; we signal
      ** outside the mutex to avoid the wake-up/relock race on Linux NPTL
      ** (recent glibc handles this well regardless, but the pattern is
      ** portable).
      */
      (void)pthread_cond_signal( &pool_xCondNotEmpty );
   }

   return( fEnqueued );
}

BOOL ABCC_API_CallbackPoolInit( void )
{
   int i;
   int xStatus;

   if( pool_fInitDone )
   {
      return( TRUE );
   }

   pool_xHead = 0;
   pool_xTail = 0;
   pool_fRunning = TRUE;

   for( i = 0; i < ABCC_API_CFG_ADI_CALLBACK_POOL_SIZE; i++ )
   {
      pool_afWorkerStarted[ i ] = FALSE;
   }

   for( i = 0; i < ABCC_API_CFG_ADI_CALLBACK_POOL_SIZE; i++ )
   {
      xStatus = pthread_create( &pool_axWorkers[ i ],
                                NULL,
                                pool_WorkerMain,
                                NULL );
      if( xStatus != 0 )
      {
         ABCC_LOG_ERROR( ABCC_EC_INTERNAL_ERROR,
                         (UINT32)xStatus,
                         "Callback pool: pthread_create worker %d failed: %s\n",
                         i, strerror( xStatus ) );
         /*
         ** Best-effort teardown of any workers that did start.
         */
         pool_fRunning = FALSE;
         (void)pthread_mutex_lock( &pool_xMutex );
         (void)pthread_cond_broadcast( &pool_xCondNotEmpty );
         (void)pthread_mutex_unlock( &pool_xMutex );

         {
            int j;
            for( j = 0; j < i; j++ )
            {
               if( pool_afWorkerStarted[ j ] )
               {
                  (void)pthread_join( pool_axWorkers[ j ], NULL );
                  pool_afWorkerStarted[ j ] = FALSE;
               }
            }
         }
         return( FALSE );
      }
      pool_afWorkerStarted[ i ] = TRUE;
   }

   pool_fInitDone = TRUE;
   ABCC_LOG_INFO( "Callback pool started: %d workers, %d queue slots\n",
                  ABCC_API_CFG_ADI_CALLBACK_POOL_SIZE,
                  POOL_QUEUE_SIZE );
   return( TRUE );
}

void ABCC_API_CallbackPoolShutdown( void )
{
   int i;

   if( !pool_fInitDone )
   {
      return;
   }

   (void)pthread_mutex_lock( &pool_xMutex );
   pool_fRunning = FALSE;
   (void)pthread_cond_broadcast( &pool_xCondNotEmpty );
   (void)pthread_mutex_unlock( &pool_xMutex );

   for( i = 0; i < ABCC_API_CFG_ADI_CALLBACK_POOL_SIZE; i++ )
   {
      if( pool_afWorkerStarted[ i ] )
      {
         (void)pthread_join( pool_axWorkers[ i ], NULL );
         pool_afWorkerStarted[ i ] = FALSE;
      }
   }

   pool_fInitDone = FALSE;
}

void ABCC_API_CallbackPoolDispatch( ABCC_API_CallbackKind       eKind,
                                    const AD_AdiEntryType*      psAdiEntry,
                                    UINT8                       bNumElements,
                                    UINT8                       bStartIndex )
{
   pool_Job sJob;

   sJob.eKind        = eKind;
   sJob.psAdiEntry   = psAdiEntry;
   sJob.bNumElements = bNumElements;
   sJob.bStartIndex  = bStartIndex;
   static BOOL fOverflowLogged = FALSE;

   if( pool_fRunning && pool_fInitDone )
   {
      if( pool_TryEnqueue( &sJob ) )
      {
        fOverflowLogged = FALSE;
         return;
      }
      /*
      ** Queue full — fall through to synchronous execution. We also log
      ** once per episode of saturation; logging every overflow would
      ** itself become a bottleneck under overload.
      */
      {
         if( !fOverflowLogged )
         {
            ABCC_LOG_WARNING( ABCC_EC_NO_RESOURCES,
                              (UINT32)POOL_QUEUE_SIZE,
                              "Callback pool queue full (size=%u); "
                              "falling back to synchronous dispatch\n",
                              (unsigned)POOL_QUEUE_SIZE );
            fOverflowLogged = TRUE;
         }
      }
   }

   pool_InvokeJob( &sJob );
}

#else  /* ABCC_API_CFG_ADI_CALLBACK_POOL_ENABLED == 0 */

BOOL ABCC_API_CallbackPoolInit( void )
{
   return( TRUE );
}

void ABCC_API_CallbackPoolShutdown( void )
{
   /* no-op */
}

void ABCC_API_CallbackPoolDispatch( ABCC_API_CallbackKind       eKind,
                                    const AD_AdiEntryType*      psAdiEntry,
                                    UINT8                       bNumElements,
                                    UINT8                       bStartIndex )
{
   if( psAdiEntry == NULL )
   {
      return;
   }

   if( eKind == ABCC_API_CB_KIND_GET )
   {
      if( psAdiEntry->pnGetAdiValue != NULL )
      {
         psAdiEntry->pnGetAdiValue( psAdiEntry, bNumElements, bStartIndex );
      }
   }
   else
   {
      if( psAdiEntry->pnSetAdiValue != NULL )
      {
         psAdiEntry->pnSetAdiValue( psAdiEntry, bNumElements, bStartIndex );
      }
   }
}

#endif  /* ABCC_API_CFG_ADI_CALLBACK_POOL_ENABLED */