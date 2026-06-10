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

let db;
if (serviceAccount) {
  try {
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount)
    });
    db = admin.firestore();
    console.log("[Push] Firebase Admin SDK e inicialización de Firestore con éxito.");
  } catch (e) {
    console.error("Error al inicializar Firebase Admin o Firestore:", e);
  }
} else {
  console.log("[Push] Firebase Admin SDK no inicializado. Las notificaciones push y persistencia no funcionarán.");
}

const path = require('path');

const app = express();
app.use(cors());

// Servir archivos estáticos del frontend (React + APK)
app.use(express.static(path.join(__dirname, 'public')));

// Redirigir cualquier otra ruta HTTP al index.html del frontend
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
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

// Fallback in-memory storage (para cuando Firestore no esté habilitado)
const offlineMessages = new Map();
const fcmTokens = new Map();

async function saveFcmToken(phantomId, token) {
  const cleanId = phantomId.toUpperCase();
  fcmTokens.set(cleanId, token);
  if (db) {
    try {
      await db.collection('fcmTokens').doc(cleanId).set({
        token: token,
        updatedAt: new Date().toISOString()
      });
      console.log(`[Firestore] Token guardado para ${cleanId}`);
    } catch (e) {
      console.warn(`[Firestore] Error al guardar token en base de datos:`, e.message);
    }
  }
}

async function getFcmToken(phantomId) {
  const cleanId = phantomId.toUpperCase();
  if (db) {
    try {
      const doc = await db.collection('fcmTokens').doc(cleanId).get();
      if (doc.exists) {
        return doc.data().token;
      }
    } catch (e) {
      console.warn(`[Firestore] Error al leer token de base de datos, usando memoria:`, e.message);
    }
  }
  return fcmTokens.get(cleanId);
}

async function queueOfflineMessage(to, msgData) {
  const cleanTo = to.toUpperCase();
  if (!offlineMessages.has(cleanTo)) {
    offlineMessages.set(cleanTo, []);
  }
  offlineMessages.get(cleanTo).push(msgData);

  if (db) {
    try {
      await db.collection('offlineMessages').add({
        ...msgData,
        to: cleanTo,
        timestamp: Date.now()
      });
      console.log(`[Firestore] Mensaje offline guardado para ${cleanTo}`);
    } catch (e) {
      console.warn(`[Firestore] Error al guardar mensaje offline en base de datos:`, e.message);
    }
  }
}

async function deliverOfflineMessages(phantomId, socket) {
  const cleanId = phantomId.toUpperCase();
  let deliveredFromDb = false;

  if (db) {
    try {
      const snapshot = await db.collection('offlineMessages')
        .where('to', '==', cleanId)
        .get();
      
      if (!snapshot.empty) {
        const docs = [];
        snapshot.forEach(doc => {
          docs.push({ id: doc.id, data: doc.data() });
        });
        docs.sort((a, b) => (a.data.timestamp || 0) - (b.data.timestamp || 0));

        console.log(`[Firestore] Entregando ${docs.length} mensaje(s) offline para ${cleanId}`);
        for (const doc of docs) {
          socket.emit('private_message', doc.data);
          await db.collection('offlineMessages').doc(doc.id).delete();
        }
        deliveredFromDb = true;
      }
    } catch (e) {
      console.warn(`[Firestore] Error al entregar desde base de datos, recurriendo a memoria:`, e.message);
    }
  }

  if (offlineMessages.has(cleanId)) {
    const queued = offlineMessages.get(cleanId);
    if (!deliveredFromDb && queued.length > 0) {
      console.log(`[Memoria] Entregando ${queued.length} mensaje(s) offline para ${cleanId}`);
      queued.forEach(data => {
        socket.emit('private_message', data);
      });
    }
    offlineMessages.delete(cleanId);
  }
}

async function sendPushNotification(recipientId, senderId) {
  const token = await getFcmToken(recipientId.toUpperCase());
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
  socket.on('register', async (phantomId) => {
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
      await deliverOfflineMessages(phantomId, socket);
    }
  });

  // Registro de token FCM para notificaciones push con la app cerrada
  socket.on('register_fcm_token', async (data) => {
    const { phantomId, token } = data;
    if (phantomId && token) {
      await saveFcmToken(phantomId, token);
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
  socket.on('private_message', async (data) => {
    const { to, from, payload, recipientFcmToken, senderFcmToken } = data;
    
    if (recipientFcmToken) {
      await saveFcmToken(to, recipientFcmToken);
    }
    if (senderFcmToken) {
      await saveFcmToken(from, senderFcmToken);
    }

    const recipientSocket = users.get(to);
    
    if (recipientSocket) {
      io.to(recipientSocket).emit('private_message', data);
      console.log(`[>] Mensaje enrutado de ${from} hacia ${to}`);
      // Si está inactivo (segundo plano), le mandamos push para asegurar que le notifique
      if (inactiveUsers.has(to)) {
        await sendPushNotification(to, from);
      }
    } else {
      console.log(`[!] Destinatario ${to} no está online. Guardando en cola de mensajes offline.`);
      await queueOfflineMessage(to, data);
      // Al estar offline (app cerrada/matada), le enviamos una notificación push
      await sendPushNotification(to, from);
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
