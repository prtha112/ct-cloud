import Redis from 'ioredis';

const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

// Ensure the connection is cached across hot-reloads in development
let redis: Redis;

if (process.env.NODE_ENV === 'production') {
    redis = new Redis(redisUrl);
} else {
    // Avoid creating multiple connections during Next.js hot reloads in dev
    const globalForRedis = global as unknown as { redis: Redis };
    if (!globalForRedis.redis) {
        globalForRedis.redis = new Redis(redisUrl);
    }
    redis = globalForRedis.redis;
}

export default redis;
