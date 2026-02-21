import { NextResponse } from 'next/server';
import redis from '@/lib/redis';

export async function GET() {
    try {
        const primaryUrl = await redis.get('mssql_sync:config:primary_url');
        const replicaUrl = await redis.get('mssql_sync:config:replica_url');

        return NextResponse.json({
            primaryUrl: primaryUrl || 'Loading...',
            replicaUrl: replicaUrl || 'Loading...',
        });
    } catch (error) {
        console.error('Failed to fetch config from Redis:', error);
        return NextResponse.json({ error: 'Internal Server Error' }, { status: 500 });
    }
}
