const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const admin = require('firebase-admin');

let serviceAccount;
if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  try {
    serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  } catch (e) {
    console.error("Error parsing FIREBASE_SERVICE_ACCOUNT environment variable:", e);
  }
} else {
  try {
    serviceAccount = require('./firebase-service-account.json');
  } catch (e) {
    console.log("firebase-service-account.json file not found, running without Firebase Admin capabilities (local dev).");
  }
}

if (serviceAccount) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
  console.log("[Push] Firebase Admin SDK inicializado con éxito.");
} else {
  console.log("[Push] Firebase Admin SDK no inicializado. Las notificaciones push no funcionarán.");
}

const app = express();
app.use(cors());

// Endpoint de estado para UptimeRobot / Monitoreo
app.get('/', (req, res) => {
  res.send('Phantom Router Status: Active');
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// Map of phantomId to socket.id
const users = new Map();
// Set of phantomIds that are connected but in the background (inactive)
const inactiveUsers = new Set();
// In-memory offline message queue (maps phantomId to Array of encrypted messages)
const offlineMessages = new Map();
// Maps phantomId to FCM token
const fcmTokens = new Map();

function sendPushNotification(recipientId, senderId) {
  const token = fcmTokens.get(recipientId.toUpperCase());
  if (!token) {
    console.log(`[Push] No hay token FCM registrado para ${recipientId.toUpperCase()}.`);
    return;
  }

  if (admin.apps.length === 0) {
    console.log(`[Push] Firebase no está inicializado. Ignorando push notification.`);
    return;
  }

  const message = {
    token: token,
    notification: {
      title: 'Nuevo mensaje cifrado',
      body: 'Has recibido un nuevo mensaje de chat seguro.'
    },
    android: {
      priority: 'high',
      notification: {
        sound: 'default',
        channelId: 'phantom_messages'
      }
    },
    apns: {
      payload: {
        aps: {
          sound: 'default'
        }
      }
    }
  };

  admin.messaging().send(message)
    .then((response) => {
      console.log(`[Push] Notificación enviada con éxito a ${recipientId.toUpperCase()}:`, response);
    })
    .catch((error) => {
      console.error(`[Push] Error al enviar notificación a ${recipientId.toUpperCase()}:`, error);
    });
}

io.on('connection', (socket) => {
  console.log('Nueva conexión entrante:', socket.id);

  // Cuando un usuario abre la app, registra su UUID secreto en el servidor
  socket.on('register', (phantomId) => {
    if (phantomId) {
      users.set(phantomId, socket.id);
      inactiveUsers.delete(phantomId); // Asegurar que empieza activo
      console.log(`[+] Identidad registrada: ${phantomId} (Socket: ${socket.id})`);
      
      // Enviar la lista de usuarios conectados al socket que se acaba de registrar (solo los activos)
      const onlineList = Array.from(users.keys()).filter(id => !inactiveUsers.has(id));
      socket.emit('online_users_list', onlineList);
      
      // Notificar a todos los demás que este usuario se ha conectado
      socket.broadcast.emit('user_online', phantomId);

      // Entregar mensajes offline en cola si existen
      if (offlineMessages.has(phantomId)) {
        const queued = offlineMessages.get(phantomId);
        console.log(`[>>] Entregando ${queued.length} mensaje(s) offline acumulado(s) para ${phantomId}`);
        queued.forEach(data => {
          socket.emit('private_message', data);
        });
        offlineMessages.delete(phantomId);
      }
    }
  });

  // Registro de token FCM para notificaciones push con la app cerrada
  socket.on('register_fcm_token', (data) => {
    const { phantomId, token } = data;
    if (phantomId && token) {
      fcmTokens.set(phantomId.toUpperCase(), token);
      console.log(`[Push] Token registrado para ${phantomId.toUpperCase()}: ${token.substring(0, 15)}...`);
    }
  });

  // Cambiar estado activo/inactivo (segundo plano) sin desconectar
  socket.on('set_active', (isActive) => {
    let userPhantomId = null;
    for (const [id, socketId] of users.entries()) {
      if (socketId === socket.id) {
        userPhantomId = id;
        break;
      }
    }

    if (userPhantomId) {
      if (isActive) {
        inactiveUsers.delete(userPhantomId);
        socket.broadcast.emit('user_online', userPhantomId);
        console.log(`[+] Usuario activo (primer plano): ${userPhantomId}`);
      } else {
        inactiveUsers.add(userPhantomId);
        io.emit('user_offline', userPhantomId);
        console.log(`[-] Usuario inactivo (segundo plano): ${userPhantomId}`);
      }
    }
  });

  // Enrutamiento de mensajes (El servidor NO lee el contenido, está cifrado en AES)
  socket.on('private_message', (data) => {
    const { to, from, payload, recipientFcmToken, senderFcmToken } = data;
    
    if (recipientFcmToken) {
      fcmTokens.set(to.toUpperCase(), recipientFcmToken);
      console.log(`[Push] Token de destinatario registrado/recuperado para ${to.toUpperCase()} desde mensaje.`);
    }
    if (senderFcmToken) {
      fcmTokens.set(from.toUpperCase(), senderFcmToken);
      console.log(`[Push] Token de remitente registrado/recuperado para ${from.toUpperCase()} desde mensaje.`);
    }

    const recipientSocket = users.get(to);
    
    if (recipientSocket) {
      io.to(recipientSocket).emit('private_message', data);
      console.log(`[>] Mensaje enrutado de ${from} hacia ${to}`);
      // Si está inactivo (segundo plano), le mandamos push para asegurar que le notifique
      if (inactiveUsers.has(to)) {
        sendPushNotification(to, from);
      }
    } else {
      console.log(`[!] Destinatario ${to} no está online. Guardando en cola de mensajes offline.`);
      if (!offlineMessages.has(to)) {
        offlineMessages.set(to, []);
      }
      offlineMessages.get(to).push(data);
      // Al estar offline (app cerrada/matada), le enviamos una notificación push
      sendPushNotification(to, from);
    }
  });

  // Telemetría de administración privada
  socket.on('get_admin_stats', (data, callback) => {
    const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'VilaKretos2005.';
    const { password } = data;
    if (password === ADMIN_PASSWORD) {
      if (typeof callback === 'function') {
        callback({
          success: true,
          onlineCount: users.size - inactiveUsers.size,
          onlineUsers: Array.from(users.keys()).filter(id => !inactiveUsers.has(id))
        });
      }
    } else {
      if (typeof callback === 'function') {
        callback({ success: false, error: 'Acceso denegado' });
      }
    }
  });

  socket.on('disconnect', () => {
    console.log('[-] Conexión cerrada:', socket.id);
    for (const [id, socketId] of users.entries()) {
      if (socketId === socket.id) {
        users.delete(id);
        inactiveUsers.delete(id);
        // Notificar a todos que este usuario se ha desconectado
        io.emit('user_offline', id);
        break;
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Phantom Secure Server (Router) escuchando en puerto ${PORT}`);
});
