import net from 'node:net';
import { execSync } from 'node:child_process';
import vue from '@vitejs/plugin-vue';
import { defineConfig, loadEnv } from 'vite';

/** @param {string} host @param {number} port */
function canConnect(host, port, timeoutMs = 500) {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host, port, family: 4 }, () => {
      socket.end();
      resolve(true);
    });
    socket.on('error', () => resolve(false));
    socket.setTimeout(timeoutMs);
    socket.on('timeout', () => {
      socket.destroy();
      resolve(false);
    });
  });
}

function firstWslIPv4() {
  try {
    const out = execSync('wsl -e hostname -I', {
      encoding: 'utf8',
      timeout: 6000,
      windowsHide: true,
    }).trim();
    const ip = out.split(/\s+/).find((t) => /^\d{1,3}(\.\d{1,3}){3}$/.test(t));
    return ip ?? null;
  } catch {
    return null;
  }
}

async function resolveApiTarget(mode) {
  const env = loadEnv(mode, process.cwd(), '');
  const explicit = env.VITE_DEV_PROXY_TARGET || process.env.VITE_DEV_PROXY_TARGET;
  if (explicit) {
    return explicit;
  }

  const fallback = 'http://127.0.0.1:8080';

  if (process.platform !== 'win32' || process.env.VITE_SKIP_WSL_PROXY_IP === '1') {
    return fallback;
  }

  if (await canConnect('127.0.0.1', 8080)) {
    return fallback;
  }

  const wslIp = firstWslIPv4();
  if (wslIp && (await canConnect(wslIp, 8080))) {
    return `http://${wslIp}:8080`;
  }

  return fallback;
}

export default defineConfig(async ({ mode }) => {
  const apiTarget = await resolveApiTarget(mode);

  return {
    plugins: [
      vue(),
      {
        name: 'log-api-proxy-target',
        configureServer() {
          console.log(`\n  \x1b[36m[api-proxy]\x1b[0m /api -> ${apiTarget}\n`);
          const u = new URL(apiTarget);
          const port = Number(u.port || 80);
          setTimeout(async () => {
            const ok = await canConnect(u.hostname, port);
            if (!ok) {
              console.warn(
                `\n  \x1b[33m[api-proxy]\x1b[0m nothing is listening at ${apiTarget} — start the Spring backend first (e.g. make backend-run).\n` +
                  `  If backend runs in WSL and you use Windows for Vite, restart Vite after the backend is up, or set VITE_DEV_PROXY_TARGET in frontend/.env.development.\n`,
              );
            }
          }, 1500);
        },
      },
    ],
    server: {
      port: 5173,
      host: true,
      proxy: {
        '/api': {
          target: apiTarget,
          changeOrigin: true,
        },
      },
    },
  };
});
