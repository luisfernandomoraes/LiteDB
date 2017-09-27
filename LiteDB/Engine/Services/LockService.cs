using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace LiteDB
{
    /// <summary>
    /// Implement simple lock service (multi-reader/single-writer [with no-reader])
    /// Use ReaderWriterLockSlim for thread lock and FileStream.Lock for file (inside disk impl)
    /// [Thread Safe]
    /// </summary>
    public class LockService
    {
		#region Properties + Ctor

		private LockState _state;
	    private bool _shared = false;

		private TimeSpan _timeout;
        private IDiskService _disk;
        private CacheService _cache;
        private Logger _log;
        private ReaderWriterLockSlim _thread = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

        internal LockService(IDiskService disk, CacheService cache, TimeSpan timeout, Logger log)
        {
            _disk = disk;
            _cache = cache;
            _log = log;
            _timeout = timeout;
        }

        /// <summary>
        /// Get current datafile lock state defined by thread only (do not check if datafile is locked)
        /// </summary>
        public LockState ThreadState
        {
            get
            {
                return _thread.IsWriteLockHeld ? LockState.Write :
                    _thread.CurrentReadCount > 0 ? LockState.Read : LockState.Unlocked;
            }
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Enter in Shared lock mode.
        /// </summary>
        public LockControl Read()
        {
            // if read or write
            if (_thread.IsReadLockHeld || _thread.IsWriteLockHeld)
            {
                return new LockControl(() => { });
            }

            // try enter in read mode
            if (!_thread.TryEnterReadLock(_timeout))
            {
                throw LiteException.LockTimeout(_timeout);
            }

            _log.Write(Logger.LOCK, "entered in read lock mode in thread #{0}", Thread.CurrentThread.ManagedThreadId);

            // lock disk in shared mode
            var position = _disk.Lock(LockState.Read, _timeout);

            this.DetectDatabaseChanges();

            return new LockControl(() =>
            {
                // exit disk lock mode
                _disk.Unlock(LockState.Read, position);

                // exit thread lock mode
                _thread.ExitReadLock();

                _log.Write(Logger.LOCK, "exited read lock mode in thread #{0}", Thread.CurrentThread.ManagedThreadId);
            });
        }

        /// <summary>
        /// Enter in Exclusive lock mode
        /// </summary>
        public LockControl Write()
        {
            // if already in exclusive, do nothing
            if (_thread.IsWriteLockHeld)
            {
                return new LockControl(() => { });
            }

            // let's test if is not in read lock
            if (_thread.IsReadLockHeld) throw new NotSupportedException("Not support Write lock inside a Read lock");

            // try enter in write mode (thread)
            if (!_thread.TryEnterWriteLock(_timeout))
            {
                throw LiteException.LockTimeout(_timeout);
            }

            _log.Write(Logger.LOCK, "entered in write lock mode in thread #{0}", Thread.CurrentThread.ManagedThreadId);

            // try enter in exclusive mode in disk
            var position = _disk.Lock(LockState.Write, _timeout);

            // call avoid dirty only if not came from a shared mode
            this.DetectDatabaseChanges();

            return new LockControl(() =>
            {
                // release disk write
                _disk.Unlock(LockState.Write, position);

                // release thread write
                _thread.ExitWriteLock();

                _log.Write(Logger.LOCK, "exited write lock mode in thread #{0}", Thread.CurrentThread.ManagedThreadId);
            });
        }

	    /// <summary>
	    /// Enter in Reserved lock mode.
	    /// </summary>
	    public LockControl Reserved()
	    {
		    var write = this.ThreadWrite();
		    var reserved = this.LockReserved();

		    return new LockControl(() =>
		    {
			    reserved();
			    write();
		    });
	    }
	    /// <summary>
	    /// Enter in Exclusive lock mode
	    /// </summary>
	    public LockControl Exclusive()
	    {
		    var exclusive = this.LockExclusive();

		    return new LockControl(exclusive);
	    }
	    /// <summary>
	    /// Try enter in exclusive mode (single write)
	    /// [ThreadSafe] - always inside Reserved() -> Write() 
	    /// </summary>
	    private Action LockExclusive()
	    {
		    lock (_disk)
		    {
			    if (_state != LockState.Reserved) throw new InvalidOperationException("Lock state must be reserved");

			    // has a shared lock? unlock first (will keep reserved lock)
			    if (_shared)
			    {
				    var position = _disk.Lock(LockState.Read, _timeout);
					_disk.Unlock(LockState.Shared,position);
			    }

			    _disk.Lock(LockState.Exclusive, _timeout);

			    _state = LockState.Exclusive;

			    _log.Write(Logger.LOCK, "entered in exclusive lock mode");

			    return () =>
			    {
				    var position = _disk.Lock(LockState.Read, _timeout);
					_disk.Unlock(LockState.Exclusive,position);
				    _state = LockState.Reserved;

				    _log.Write(Logger.LOCK, "exited exclusive lock mode");

				    // if was in a shared lock before exclusive lock, back to shared again (still reserved lock)
				    if (_shared)
				    {
					    _disk.Lock(LockState.Shared, _timeout);

					    _log.Write(Logger.LOCK, "backed to shared mode");
				    }
			    };
		    }
	    }
		/// <summary>
		/// Try enter in reserved mode (read - single reserved)
		/// [ThreadSafe] (always inside an Write())
		/// </summary>
		private Action LockReserved()
	    {
		    lock (_disk)
		    {
			    if (_state == LockState.Reserved) return () => { };

			    _disk.Lock(LockState.Reserved, _timeout);

			    _state = LockState.Reserved;

			    _log.Write(Logger.LOCK, "entered in reserved lock mode");

			    // can be a new lock, calls action to notifify
			    if (!_shared)
			    {
				    this.AvoidDirtyRead();
			    }

			    // is new lock only when not came from a shared lock
			    return () =>
			    {
				    var position = _disk.Lock(LockState.Read, _timeout);
					_disk.Unlock(LockState.Reserved,position);

				    _state = _shared ? LockState.Shared : LockState.Unlocked;

				    _log.Write(Logger.LOCK, "exited reserved lock mode");
			    };
		    }
	    }
		/// <summary>
		/// Start new exclusive write lock control using timeout
		/// </summary>
		private Action ThreadWrite()
	    {
		    // if current thread is already in write mode, do nothing
		    if (_thread.IsWriteLockHeld) return () => { };

		    // if current thread is in read mode, exit read mode first
		    if (_thread.IsReadLockHeld)
		    {
			    _thread.ExitReadLock();
			    _thread.TryEnterWriteLock(_timeout);

			    // when dispose write mode, enter again in read mode
			    return () =>
			    {
				    _thread.ExitWriteLock();
				    _thread.TryEnterReadLock(_timeout);
			    };
		    }

		    // try enter in write mode
		    if (!_thread.TryEnterWriteLock(_timeout))
		    {
			    throw LiteException.LockTimeout(_timeout);
		    }

		    // and release when dispose
		    return () => _thread.ExitWriteLock();
	    }
		#endregion
	    private void AvoidDirtyRead()
	    {
		    // if disk are exclusive don't need check dirty read
		    if (_disk.IsExclusive) return;

		    _log.Write(Logger.CACHE, "checking disk to avoid dirty read");

		    // empty cache? just exit
		    if (_cache.CleanUsed == 0) return;

		    // get ChangeID from cache
		    var header = _cache.GetPage(0) as HeaderPage;
		    var changeID = header == null ? 0 : header.ChangeID;

		    // and get header from disk
		    var disk = BasePage.ReadPage(_disk.ReadPage(0)) as HeaderPage;

		    // if header change, clear cache and add new header to cache
		    if (disk.ChangeID != changeID)
		    {
			    _log.Write(Logger.CACHE, "file changed from another process, cleaning all cache pages");

			    _cache.ClearPages();
			    _cache.AddPage(disk);
		    }
	    }
		/// <summary>
		/// Test if cache still valid (if datafile was changed by another process reset cache)
		/// [Thread Safe]
		/// </summary>
		private void DetectDatabaseChanges()
        {
            // if disk are exclusive don't need check dirty read
            if (_disk.IsExclusive) return;

            _log.Write(Logger.CACHE, "checking disk to avoid dirty read");

            // empty cache? just exit
            if (_cache.CleanUsed == 0) return;

            // get ChangeID from cache
            var header = _cache.GetPage(0) as HeaderPage;
            var changeID = header == null ? 0 : header.ChangeID;

            // and get header from disk
            var disk = BasePage.ReadPage(_disk.ReadPage(0)) as HeaderPage;

            // if disk header are in recovery mode, throw exception to datafile re-open and recovery pages
            if (disk.Recovery)
            {
                _log.Write(Logger.ERROR, "datafile in recovery mode, need re-open database");

                throw LiteException.NeedRecover();
            }

            // if header change, clear cache and add new header to cache
            if (disk.ChangeID != changeID)
            {
                _log.Write(Logger.CACHE, "file changed from another process, cleaning all cache pages");

                _cache.ClearPages();
                _cache.AddPage(disk);
            }
        }
    }
}