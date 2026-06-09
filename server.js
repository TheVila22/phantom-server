const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

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
// In-memory offline message queue (maps phantomId to Array of encrypted messages)
const offlineMessages = new Map();

io.on('connection', (socket) => {
  console.log('Nueva conexión entrante:', socket.id);

  // Cuando un usuario abre la app, registra su UUID secreto en el servidor
  socket.on('register', (phantomId) => {
    if (phantomId) {
      users.set(phantomId, socket.id);
      console.log(`[+] Identidad registrada: ${phantomId} (Socket: ${socket.id})`);
      
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

  // Enrutamiento de mensajes (El servidor NO lee el contenido, está cifrado en AES)
  socket.on('private_message', (data) => {
    const { to, from, payload } = data;
    const recipientSocket = users.get(to);
    
    if (recipientSocket) {
      io.to(recipientSocket).emit('private_message', data);
      console.log(`[>] Mensaje enrutado de ${from} hacia ${to}`);
    } else {
      console.log(`[!] Destinatario ${to} no está online. Guardando en cola de mensajes offline.`);
      if (!offlineMessages.has(to)) {
        offlineMessages.set(to, []);
      }
      offlineMessages.get(to).push(data);
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
          onlineCount: users.size,
          onlineUsers: Array.from(users.keys())
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
        break;
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Phantom Secure Server (Router) escuchando en puerto ${PORT}`);
});
