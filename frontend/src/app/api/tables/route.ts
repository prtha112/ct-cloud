import { NextResponse } from 'next/server';
import redis from '@/lib/redis';

export async function GET() {
    try {
        // Find all tracked tables by checking the enabled keys
        const keys = await redis.keys('mssql_sync:enabled:*');

        if (keys.length === 0) {
            return NextResponse.json({ tables: [] });
        }

        // Extract table names from keys (e.g. mssql_sync:enabled:User -> User)
        const tableNames = keys.map((key) => key.split(':').pop() || '');

        // Fetch the enabled, force_full_load, and version states for all tables in a single pipeline
        const pipeline = redis.pipeline();
        tableNames.forEach((table) => {
            pipeline.get(`mssql_sync:enabled:${table}`);
            pipeline.get(`mssql_sync:force_full_load:${table}`);
            pipeline.get(`mssql_sync:version:${table}`);
        });

        const results = await pipeline.exec();

        if (!results) {
            throw new Error("Pipeline execution failed");
        }

        const tables = tableNames.map((tableName, index) => {
            // Results are returned as [error, value] arrays
            const enabledVal = results[index * 3][1] as string | null;
            const forceLoadVal = results[index * 3 + 1][1] as string | null;
            const versionVal = results[index * 3 + 2][1] as string | null;

            return {
                id: tableName,
                name: tableName,
                enabled: enabledVal === 'true',
                forceFullLoad: forceLoadVal === 'true',
                version: versionVal ? parseInt(versionVal, 10) : 0,
            };
        });

        // Sort alphabetically by table name
        tables.sort((a, b) => a.name.localeCompare(b.name));

        return NextResponse.json({ tables });
    } catch (error) {
        console.error('Failed to fetch tables from Redis:', error);
        return NextResponse.json({ error: 'Internal Server Error' }, { status: 500 });
    }
}
