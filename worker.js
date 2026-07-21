// HackFire's Panel — Main Worker (Step 1)
// Single-user VLESS-over-WebSocket relay on Cloudflare Workers.
// No dashboard yet — this is the bare relay core everything else builds on.

import { connect } from 'cloudflare:sockets';

// Set this in wrangler.jsonc vars, or Cloudflare dashboard > Settings > Variables.
// Generate one with: node -e "console.log(crypto.randomUUID())"
const FALLBACK_USER_ID = '00000000-0000-0000-0000-000000000000';

export default {
  async fetch(request, env, ctx) {
    try {
      const userID = (env.USER_ID || FALLBACK_USER_ID).trim();
      const upgradeHeader = request.headers.get('Upgrade');

      if (upgradeHeader === 'websocket') {
        return await handleVlessOverWs(request, userID);
      }

      // Plain HTTP request — show a minimal status/info page instead of erroring.
      const url = new URL(request.url);
      if (url.pathname === `/${userID}`) {
        return new Response(buildInfoPage(request, userID), {
          headers: { 'content-type': 'text/plain;charset=utf-8' },
        });
      }

      return new Response('Not found', { status: 404 });
    } catch (err) {
      return new Response(`Error: ${err.message}`, { status: 500 });
    }
  },
};

// ---- Core VLESS + WebSocket relay ----

async function handleVlessOverWs(request, userID) {
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);

  server.accept();

  let remoteSocket = { value: null };
  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
  const readableStream = makeWebSocketReadable(server, earlyDataHeader);

  readableStream
    .pipeTo(
      new WritableStream({
        async write(chunk) {
          if (remoteSocket.value) {
            const writer = remoteSocket.value.writable.getWriter();
            await writer.write(chunk);
            writer.releaseLock();
            return;
          }

          const { hasError, message, portRemote, addressRemote, rawDataIndex, vlessVersion, isUDP } =
            parseVlessHeader(chunk, userID);

          if (hasError) {
            throw new Error(message);
          }

          if (isUDP) {
            // DNS-over-UDP-in-VLESS is common for port 53; anything else UDP is unsupported here.
            if (portRemote !== 53) {
              throw new Error('UDP only supported on port 53 in this build');
            }
          }

          const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
          const rawClientData = chunk.slice(rawDataIndex);

          await handleTcpOutbound({
            remoteSocket,
            addressRemote,
            portRemote,
            rawClientData,
            server,
            vlessResponseHeader,
          });
        },
        close() {
          if (remoteSocket.value) remoteSocket.value.close().catch(() => {});
        },
        abort() {
          if (remoteSocket.value) remoteSocket.value.close().catch(() => {});
        },
      })
    )
    .catch(() => {
      safeCloseWebSocket(server);
    });

  return new Response(null, { status: 101, webSocket: client });
}

async function handleTcpOutbound({ remoteSocket, addressRemote, portRemote, rawClientData, server, vlessResponseHeader }) {
  const tcpSocket = connect({ hostname: addressRemote, port: portRemote });
  remoteSocket.value = tcpSocket;

  const writer = tcpSocket.writable.getWriter();
  await writer.write(rawClientData);
  writer.releaseLock();

  let headerSent = false;
  await tcpSocket.readable
    .pipeTo(
      new WritableStream({
        write(chunk) {
          if (server.readyState !== 1 /* OPEN */) return;
          if (!headerSent) {
            const combined = new Uint8Array(vlessResponseHeader.length + chunk.byteLength);
            combined.set(vlessResponseHeader, 0);
            combined.set(new Uint8Array(chunk), vlessResponseHeader.length);
            server.send(combined);
            headerSent = true;
          } else {
            server.send(chunk);
          }
        },
        close() {
          safeCloseWebSocket(server);
        },
        abort() {
          safeCloseWebSocket(server);
        },
      })
    )
    .catch(() => {
      safeCloseWebSocket(server);
    });
}

// ---- VLESS header parsing (protocol ref: xtls/xray VLESS spec) ----

function parseVlessHeader(buffer, userID) {
  if (buffer.byteLength < 24) {
    return { hasError: true, message: 'invalid header: too short' };
  }

  const view = new DataView(buffer);
  const vlessVersion = new Uint8Array(buffer.slice(0, 1));

  const uuidBytes = new Uint8Array(buffer.slice(1, 17));
  const requestUUID = bytesToUuid(uuidBytes);
  if (requestUUID !== userID) {
    return { hasError: true, message: 'invalid user' };
  }

  const optionsLength = new Uint8Array(buffer.slice(17, 18))[0];
  const command = new Uint8Array(buffer.slice(18 + optionsLength, 18 + optionsLength + 1))[0];

  let isUDP = false;
  if (command === 1) {
    // TCP
  } else if (command === 2) {
    isUDP = true;
  } else {
    return { hasError: true, message: `unsupported command: ${command}` };
  }

  const portIndex = 18 + optionsLength + 1;
  const portRemote = view.getUint16(portIndex);

  const addressIndex = portIndex + 2;
  const addressType = new Uint8Array(buffer.slice(addressIndex, addressIndex + 1))[0];

  let addressLength = 0;
  let addressValueIndex = addressIndex + 1;
  let addressValue = '';

  switch (addressType) {
    case 1: // IPv4
      addressLength = 4;
      addressValue = new Uint8Array(buffer.slice(addressValueIndex, addressValueIndex + addressLength)).join('.');
      break;
    case 2: // domain
      addressLength = new Uint8Array(buffer.slice(addressValueIndex, addressValueIndex + 1))[0];
      addressValueIndex += 1;
      addressValue = new TextDecoder().decode(buffer.slice(addressValueIndex, addressValueIndex + addressLength));
      break;
    case 3: { // IPv6
      addressLength = 16;
      const dataView = new DataView(buffer.slice(addressValueIndex, addressValueIndex + addressLength));
      const parts = [];
      for (let i = 0; i < 8; i++) parts.push(dataView.getUint16(i * 2).toString(16));
      addressValue = parts.join(':');
      break;
    }
    default:
      return { hasError: true, message: `unsupported address type: ${addressType}` };
  }

  if (!addressValue) {
    return { hasError: true, message: 'empty address' };
  }

  const rawDataIndex = addressValueIndex + addressLength;

  return {
    hasError: false,
    addressRemote: addressValue,
    addressType,
    portRemote,
    rawDataIndex,
    vlessVersion,
    isUDP,
  };
}

function bytesToUuid(bytes) {
  const hex = [...bytes].map((b) => b.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

// ---- WebSocket <-> ReadableStream bridge ----

function makeWebSocketReadable(webSocket, earlyDataHeader) {
  let cancelled = false;

  return new ReadableStream({
    start(controller) {
      webSocket.addEventListener('message', (event) => {
        if (cancelled) return;
        controller.enqueue(event.data);
      });

      webSocket.addEventListener('close', () => {
        if (!cancelled) controller.close();
      });

      webSocket.addEventListener('error', (err) => {
        controller.error(err);
      });

      const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
      if (error) {
        controller.error(error);
      } else if (earlyData) {
        controller.enqueue(earlyData);
      }
    },
    cancel() {
      cancelled = true;
      safeCloseWebSocket(webSocket);
    },
  });
}

function base64ToArrayBuffer(base64Str) {
  if (!base64Str) return { earlyData: null, error: null };
  try {
    base64Str = base64Str.replace(/-/g, '+').replace(/_/g, '/');
    const decoded = atob(base64Str);
    const arrayBuffer = Uint8Array.from(decoded, (c) => c.charCodeAt(0));
    return { earlyData: arrayBuffer.buffer, error: null };
  } catch (error) {
    return { earlyData: null, error };
  }
}

function safeCloseWebSocket(socket) {
  try {
    if (socket.readyState === 1 || socket.readyState === 2) socket.close();
  } catch (e) {
    // ignore
  }
}

function buildInfoPage(request, userID) {
  const url = new URL(request.url);
  const host = url.hostname;
  const vlessLink = `vless://${userID}@${host}:443?encryption=none&security=tls&sni=${host}&type=ws&host=${host}&path=%2F#HackFiresPanel`;
  return [
    'HackFires Panel — main worker is alive.',
    '',
    `Host: ${host}`,
    `User ID: ${userID}`,
    '',
    'Subscription link (VLESS+WS+TLS):',
    vlessLink,
  ].join('\n');
}
