/** @type {import('next').NextConfig} */
const requiredEnv = ['NEXT_PUBLIC_SITE_URL'];

for (const key of requiredEnv) {
  const value = process.env[key];
  if (!value || !value.trim()) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
}

const nextConfig = {};

export default nextConfig;
