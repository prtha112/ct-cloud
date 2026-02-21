import { NextResponse } from 'next/server';
import redis from '@/lib/redis';

export async function POST(
    request: Request,
    { params }: { params: Promise<{ id: string }> }
) {
    try {
        const tableId = (await params).id;
        const body = await request.json();
        const action = body.action;

        if (!tableId || !action) {
            return NextResponse.json({ error: 'Missing table ID or action' }, { status: 400 });
        }

        if (action === 'toggle_enabled') {
            const currentKey = `mssql_sync:enabled:${tableId}`;
            const currentState = await redis.get(currentKey);

            // Toggle string boolean
            const newState = currentState === 'true' ? 'false' : 'true';
            await redis.set(currentKey, newState);

            return NextResponse.json({ success: true, table: tableId, action, newState: newState === 'true' });

        } else if (action === 'trigger_full_load') {
            const currentKey = `mssql_sync:force_full_load:${tableId}`;

            // Set the force_full_load string to true (The Rust app acts on it and sets it to false when done)
            await redis.set(currentKey, 'true');

            return NextResponse.json({ success: true, table: tableId, action, newState: true });
        }

        return NextResponse.json({ error: 'Invalid action provided' }, { status: 400 });

    } catch (error) {
        console.error(`Failed to execute action on Redis:`, error);
        return NextResponse.json({ error: 'Internal Server Error' }, { status: 500 });
    }
}
