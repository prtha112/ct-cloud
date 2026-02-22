"use client";

import { useEffect, useState, useRef } from "react";
import { Toaster, toast } from "react-hot-toast";
import Image from "next/image";
import { Database, RefreshCw, AlertCircle, PlaySquare, ToggleLeft, ToggleRight, ArrowRight } from "lucide-react";

interface TableSyncState {
  id: string;
  name: string;
  enabled: boolean;
  forceFullLoad: boolean;
  version: number;
  progress: { synced: number; total: number; startedAt?: number; updatedAt?: number } | null;
}

interface AppConfig {
  primaryUrl: string;
  replicaUrl: string;
}

export default function Dashboard() {
  const [tables, setTables] = useState<TableSyncState[]>([]);
  const [config, setConfig] = useState<AppConfig | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [actingOn, setActingOn] = useState<string | null>(null);
  const [confirmTable, setConfirmTable] = useState<string | null>(null);
  const prevTablesRef = useRef<TableSyncState[]>([]);
  const loadStartTimesRef = useRef<Record<string, number>>({});

  const fetchTables = async () => {
    try {
      const res = await fetch("/api/tables");
      if (!res.ok) throw new Error("Failed to fetch tables");

      const data = await res.json();
      const newTables: TableSyncState[] = data.tables || [];

      // Detect Full Load Completions 
      newTables.forEach(t => {
        const prevTable = prevTablesRef.current.find(pt => pt.id === t.id);

        // Track when a full load starts if not explicitly triggered via the button
        if ((!prevTable || prevTable.forceFullLoad === false) && t.forceFullLoad === true) {
          if (!loadStartTimesRef.current[t.id]) {
            loadStartTimesRef.current[t.id] = Date.now();
          }
        }

        // If it was force loading before, but is no longer.
        if (prevTable && prevTable.forceFullLoad === true && t.forceFullLoad === false) {
          // Use server-side timestamps to prevent Docker VM clock drift throwing off the timer
          let elapsedMs = 0;
          if (t.progress?.updatedAt && t.progress?.startedAt) {
            elapsedMs = t.progress.updatedAt - t.progress.startedAt;
          } else if (loadStartTimesRef.current[t.id]) {
            elapsedMs = Date.now() - loadStartTimesRef.current[t.id];
          }

          let timeMsg = "";
          if (elapsedMs > 0) {
            const totalSeconds = Math.max(1, Math.floor(elapsedMs / 1000));
            const minutes = Math.floor(totalSeconds / 60);
            const seconds = totalSeconds % 60;
            if (minutes > 0) {
              timeMsg = ` in ${minutes}m ${seconds}s`;
            } else {
              timeMsg = ` in ${seconds}s`;
            }
          }

          // Clean up the ref
          delete loadStartTimesRef.current[t.id];

          toast.success(`Full load completed for ${t.name}${timeMsg}!`, {
            duration: 5000,
            position: 'top-right',
            style: {
              background: '#171717',
              color: '#fff',
              border: '1px solid #262626',
            },
            iconTheme: {
              primary: '#10b981',
              secondary: '#fff',
            },
          });
        }
      });

      prevTablesRef.current = newTables;
      setTables(newTables);
      setError(null);
    } catch (err: unknown) {
      if (err instanceof Error) {
        setError(err.message || "An unexpected error occurred.");
      } else {
        setError("An unexpected error occurred.");
      }
    } finally {
      setIsLoading(false);
    }
  };

  const fetchConfig = async () => {
    try {
      const res = await fetch("/api/config");
      if (res.ok) {
        const data = await res.json();
        setConfig(data);
      }
    } catch (err) {
      console.error("Failed to fetch config", err);
    }
  };

  useEffect(() => {
    fetchConfig();
    fetchTables();
    // Auto refresh every 5 seconds to get live status
    const interval = setInterval(fetchTables, 2000);
    return () => clearInterval(interval);
  }, []);

  const toggleEnabled = async (tableId: string) => {
    setActingOn(tableId);
    try {
      const res = await fetch(`/api/tables/${tableId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "toggle_enabled" })
      });

      if (res.ok) {
        const { newState } = await res.json();
        setTables(prev => prev.map(t => t.id === tableId ? { ...t, enabled: newState } : t));
      }
    } catch (err) {
      console.error("Failed to toggle state", err);
    } finally {
      setActingOn(null);
    }
  };

  const triggerFullLoad = async (tableId: string) => {
    setConfirmTable(null); // Close modal
    setActingOn(tableId + "_full");
    try {
      const res = await fetch(`/api/tables/${tableId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "trigger_full_load" })
      });

      if (res.ok) {
        loadStartTimesRef.current[tableId] = Date.now();
        setTables(prev => prev.map(t => t.id === tableId ? { ...t, forceFullLoad: true } : t));
      }
    } catch (err) {
      console.error("Failed to trigger full load", err);
    } finally {
      setActingOn(null);
    }
  };

  return (
    <div className="min-h-screen bg-neutral-950 text-neutral-200 font-sans selection:bg-blue-500/30">
      <Toaster />

      {/* Header */}
      <header className="border-b border-neutral-800 bg-neutral-900/50 backdrop-blur-xl sticky top-0 z-10">
        <div className="max-w-6xl mx-auto px-6 py-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div className="flex items-center space-x-3">
            <div className="flex items-center justify-center shrink-0">
              <Image
                src="/c758aad8-6cc9-4f28-8647-8adbe9c707e7.png"
                alt="Logo"
                width={120}
                height={40}
                className="object-contain h-12 w-auto"
                priority
              />
            </div>
            <div>
              <h1 className="text-lg font-medium text-white tracking-tight">CT-Cloud MSSQL Sync Controller</h1>

              {/* Database Connection Flow Indicator */}
              {config && (
                <div className="flex flex-col md:flex-row md:items-center mt-1 text-xs font-mono text-neutral-400 gap-1.5 md:gap-3">
                  <div className="flex items-center space-x-1.5 bg-neutral-900 border border-neutral-800 rounded-md px-2 py-0.5">
                    <Database className="w-3 h-3 text-neutral-500" />
                    <span className="truncate max-w-[150px] md:max-w-xs" title={config.primaryUrl}>
                      {config.primaryUrl.replace("mssql://", "")}
                    </span>
                  </div>

                  <ArrowRight className="w-3.5 h-3.5 text-neutral-600 hidden md:block" />

                  <div className="flex items-center space-x-1.5 bg-neutral-900 border border-neutral-800 rounded-md px-2 py-0.5">
                    <Database className="w-3 h-3 text-neutral-500" />
                    <span className="truncate max-w-[150px] md:max-w-xs" title={config.replicaUrl}>
                      {config.replicaUrl.replace("mssql://", "")}
                    </span>
                  </div>
                </div>
              )}
            </div>
          </div>

          <div className="flex items-center space-x-2 text-sm text-neutral-400">
            <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse shadow-[0_0_8px_rgba(16,185,129,0.5)]"></div>
            <span className="animate-pulse duration-1000">Live Sync Active</span>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-6xl mx-auto px-6 py-12">
        <div className="mb-8 flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div>
            <h2 className="text-3xl font-semibold text-white tracking-tight mb-2">Tracked Tables</h2>
            <p className="text-neutral-400">Manage synchronization state and trigger force loads for individual database tables.</p>
          </div>
          <button
            onClick={fetchTables}
            className="p-2.5 rounded-xl border border-neutral-800 bg-neutral-900 hover:bg-neutral-800 hover:border-neutral-700 transition-all text-neutral-400 hover:text-white flex-shrink-0"
          >
            <RefreshCw className={`w-5 h-5 ${isLoading ? 'animate-spin' : ''}`} />
          </button>
        </div>

        {error && (
          <div className="mb-8 p-4 rounded-xl bg-red-500/10 border border-red-500/20 flex items-start space-x-3 text-red-400">
            <AlertCircle className="w-5 h-5 shrink-0 mt-0.5" />
            <p>{error}</p>
          </div>
        )}

        {/* Table List */}
        <div className="bg-neutral-900/40 border border-neutral-800 rounded-2xl overflow-hidden shadow-2xl">
          <div className="grid grid-cols-12 gap-4 p-4 border-b border-neutral-800 bg-neutral-900/60 text-xs font-medium text-neutral-500 uppercase tracking-wider">
            <div className="col-span-12 md:col-span-5 flex items-center space-x-2">
              <Database className="w-3.5 h-3.5" />
              <span>Table Name</span>
            </div>
            <div className="hidden md:block col-span-2 text-center">CT Version</div>
            <div className="hidden md:block col-span-2 text-center">Sync Status</div>
            <div className="hidden md:block col-span-3 text-right pr-4">Actions</div>
          </div>

          <div className="divide-y divide-neutral-800/60">
            {isLoading && tables.length === 0 ? (
              <div className="p-12 pl-4 text-center text-neutral-500 flex flex-col items-center justify-center">
                <RefreshCw className="w-8 h-8 animate-spin mb-4 text-neutral-700" />
                <p>Loading table definitions from Redis...</p>
              </div>
            ) : tables.length === 0 ? (
              <div className="p-12 text-center text-neutral-500">
                <p>No tables found. Ensure the sync service is running and connected.</p>
              </div>
            ) : (
              tables.map(table => (
                <div key={table.id} className="grid grid-cols-1 md:grid-cols-12 gap-4 p-4 items-center hover:bg-neutral-800/30 transition-colors group">

                  {/* Table Name */}
                  <div className="col-span-1 md:col-span-5 flex items-center space-x-3">
                    <div className={`w-8 h-8 rounded-full flex shrink-0 items-center justify-center border ${table.enabled
                      ? 'bg-blue-500/10 border-blue-500/20 text-blue-400'
                      : 'bg-neutral-800/50 border-neutral-700 text-neutral-500'
                      }`}>
                      <Database className="w-4 h-4" />
                    </div>
                    <div className="min-w-0">
                      <h3 className="text-sm font-medium text-white truncate">{table.name}</h3>
                      <p className="text-xs text-neutral-500 font-mono mt-0.5 truncate hidden md:block">mssql_sync:enabled:{table.id}</p>
                    </div>
                  </div>

                  {/* CT Version */}
                  <div className="col-span-1 flex justify-between md:hidden py-2 border-y border-neutral-800 text-sm text-neutral-400">
                    <span>CT Version:</span>
                    <span className="font-mono text-neutral-300">v{table.version}</span>
                  </div>
                  <div className="hidden md:flex col-span-2 justify-center">
                    <div className="inline-flex items-center px-2.5 py-1 rounded-md bg-neutral-900 border border-neutral-800">
                      <span className="text-xs font-mono text-neutral-300">v{table.version}</span>
                    </div>
                  </div>

                  {/* Sync Status / Progress */}
                  <div className="col-span-1 md:col-span-2 flex flex-col justify-center items-center py-2 md:py-0 border-b md:border-b-0 border-neutral-800">
                    <span className="text-sm text-neutral-400 md:hidden mb-2">Sync Status:</span>

                    <button
                      onClick={() => toggleEnabled(table.id)}
                      disabled={actingOn === table.id}
                      title={table.enabled ? "Disable Sync" : "Enable Sync"}
                      className={`relative inline-flex items-center transition-all ${table.enabled
                        ? 'text-blue-500 hover:text-blue-400'
                        : 'text-neutral-600 hover:text-neutral-500'
                        } ${actingOn === table.id ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}`}
                    >
                      {table.enabled ? (
                        <ToggleRight className="w-10 h-10" strokeWidth={1.5} />
                      ) : (
                        <ToggleLeft className="w-10 h-10" strokeWidth={1.5} />
                      )}
                    </button>

                    {table.enabled && table.progress && (
                      <div className="mt-1 w-full max-w-[160px] mx-auto text-center" title={`${table.progress.synced.toLocaleString()} / ${table.progress.total.toLocaleString()} rows`}>
                        <div className="flex justify-between text-[10px] text-neutral-500 font-mono mb-1">
                          <span>{table.progress.synced >= table.progress.total ? 'Synced' : 'Loading'}</span>
                          <span>{table.progress.synced.toLocaleString()} / {table.progress.total.toLocaleString()}</span>
                        </div>
                        <div className="h-1.5 w-full bg-neutral-800 rounded-full overflow-hidden">
                          <div
                            className={`h-full rounded-full transition-all duration-500 ${table.progress.synced >= table.progress.total ? 'bg-emerald-500' : 'bg-blue-500 animate-pulse'}`}
                            style={{ width: `${table.progress.total > 0 ? Math.min(100, (table.progress.synced / table.progress.total) * 100) : 0}%` }}
                          />
                        </div>
                      </div>
                    )}
                  </div>

                  {/* Actions */}
                  <div className="col-span-1 md:col-span-3 flex justify-between md:justify-end items-center md:pr-2 pt-2 md:pt-0">
                    <span className="text-sm text-neutral-400 md:hidden">Force Full Load:</span>
                    {table.forceFullLoad ? (
                      <div className="inline-flex items-center space-x-2 px-3 py-1.5 rounded-lg bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-sm">
                        <RefreshCw className="w-4 h-4 animate-spin" />
                        <span>Loading...</span>
                      </div>
                    ) : (
                      <button
                        onClick={() => setConfirmTable(table.id)}
                        disabled={actingOn === table.id + "_full" || !table.enabled}
                        className={`inline-flex items-center space-x-2 px-4 py-1.5 rounded-lg text-sm font-medium transition-all ${!table.enabled
                          ? 'bg-neutral-900 text-neutral-600 border border-neutral-800/50 cursor-not-allowed'
                          : 'bg-white text-neutral-950 hover:bg-neutral-200 hover:scale-105 active:scale-95 shadow-[0_0_15px_rgba(255,255,255,0.1)]'
                          }`}
                        title={!table.enabled ? "Enable sync first to trigger full load" : "Trigger Full Load"}
                      >
                        <PlaySquare className="w-4 h-4" />
                        <span>Force Load</span>
                      </button>
                    )}
                  </div>

                </div>
              ))
            )}
          </div>
        </div>
      </main>

      {/* Confirmation Modal */}
      {confirmTable && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm animate-in fade-in duration-200">
          <div className="bg-neutral-900 border border-neutral-800 rounded-2xl shadow-2xl w-full max-w-md overflow-hidden animate-in zoom-in-95 duration-200">
            <div className="p-6">
              <div className="flex items-center space-x-4 text-amber-500 mb-4">
                <AlertCircle className="w-8 h-8" />
                <h3 className="text-xl font-semibold text-white">Confirm Force Load</h3>
              </div>
              <p className="text-neutral-400 mb-6">
                Are you sure you want to trigger a Full Load for <strong className="text-white">{confirmTable}</strong>?
                <br /><br />
                This action will <strong className="text-rose-400">TRUNCATE</strong> the replica table and reload all current data from the primary database.
              </p>

              <div className="flex justify-end space-x-3">
                <button
                  onClick={() => setConfirmTable(null)}
                  className="px-4 py-2 rounded-lg text-sm font-medium text-neutral-300 hover:bg-neutral-800 transition-colors"
                >
                  Cancel
                </button>
                <button
                  onClick={() => triggerFullLoad(confirmTable)}
                  className="px-4 py-2 rounded-lg text-sm font-medium bg-amber-500 hover:bg-amber-400 text-amber-950 transition-colors flex items-center space-x-2"
                >
                  <RefreshCw className="w-4 h-4" />
                  <span>Execute Full Load</span>
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

    </div>
  );
}
