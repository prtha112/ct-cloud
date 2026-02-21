import Redis from 'ioredis';

const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

// Ensure the connection is cached across hot-reloads in development
let redis: Redis;

if (process.env.NODE_ENV === 'production') {
    redis = new Redis(redisUrl);
} else {
    if (!(global as any).redis) {
        (global as any).redis = new Redis(redisUrl);
    }
    redis = (global as any).redis;
}

export default redis;
