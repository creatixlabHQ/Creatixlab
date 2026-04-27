const net = require("net");
const readline = require("readline");
const crypto = require("crypto");
const fs = require("fs");
const os = require("os");

// =====================
// ⚙️ Configuration
// =====================
const PORT = process.env.PORT || 3001;
const SEED_NODES = (process.env.SEED_NODES || "").split(",").filter(Boolean);
const MAX_RETRY = 5;
const RETRY_DELAY = 5000;

// =====================
// 📁 Persistent Storage
// =====================
let USER_ID, USERNAME, KNOWN_PEERS = new Set();

if (fs.existsSync("chat.json")) {
  const data = JSON.parse(fs.readFileSync("chat.json"));
  USER_ID = data.userId;
  USERNAME = data.username;
  if (data.knownPeers) data.knownPeers.forEach(p => KNOWN_PEERS.add(p));
} else {
  USER_ID = crypto.randomBytes(4).toString("hex");
  USERNAME = `user-${PORT}`;
  
  fs.writeFileSync("chat.json", JSON.stringify({ 
    userId: USER_ID, 
    username: USERNAME,
    knownPeers: []
  }, null, 2));
  
  console.log("📁 chat.json created");
}

// =====================
// 🌐 Network State
// =====================
const sockets = new Map(); // socket -> {peerAddress, retryCount}
const seen = new Set();
let currentRoom = "world";
let roomPassword = "";
let reconnectTimers = new Map();

// =====================
// 🔐 Encryption
// =====================
function getKey(password) {
  return crypto.createHash("sha256").update(password).digest();
}

function encrypt(text, password) {
  if (!password) return text;
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv("aes-256-cbc", getKey(password), iv);
  let encrypted = cipher.update(text, "utf8", "hex");
  encrypted += cipher.final("hex");
  return iv.toString("hex") + ":" + encrypted;
}

function decrypt(data, password) {
  if (!password) return data;
  try {
    const [ivHex, encrypted] = data.split(":");
    const iv = Buffer.from(ivHex, "hex");
    const decipher = crypto.createDecipheriv("aes-256-cbc", getKey(password), iv);
    let decrypted = decipher.update(encrypted, "hex", "utf8");
    decrypted += decipher.final("utf8");
    return decrypted;
  } catch {
    return null;
  }
}

// =====================
// 📡 Peer Discovery
// =====================
function discoverPeers() {
  console.log("\n🔍 Discovering peers...");
  
  // Broadcast discovery message to all connected peers
  const discoveryMsg = {
    type: "discovery",
    userId: USER_ID,
    port: PORT,
    timestamp: Date.now()
  };
  
  sockets.forEach((_, socket) => {
    try {
      socket.write(JSON.stringify(discoveryMsg));
    } catch(e) {}
  });
}

// =====================
// 🔗 Connection Manager with Auto-retry
// =====================
function connectToPeer(address, isSeed = false) {
  const [host, port] = address.split(":");
  
  // Check if already connected
  for (let [socket, info] of sockets.entries()) {
    if (info.address === address) {
      console.log(`⚠️ Already connected to ${address}`);
      return;
    }
  }
  
  console.log(`🔌 Connecting to ${address}...`);
  
  const socket = net.createConnection({ host, port });
  const retryCount = 0;
  
  sockets.set(socket, { address, retryCount, isSeed });
  
  socket.on("connect", () => {
    console.log(`✅ Connected to ${address}`);
    
    // Send handshake
    const handshake = {
      type: "handshake",
      userId: USER_ID,
      username: USERNAME,
      port: PORT,
      knownPeers: Array.from(KNOWN_PEERS)
    };
    socket.write(JSON.stringify(handshake));
    
    // Clear reconnect timer if exists
    if (reconnectTimers.has(address)) {
      clearTimeout(reconnectTimers.get(address));
      reconnectTimers.delete(address);
    }
  });
  
  socket.on("data", (data) => {
    try {
      const msg = JSON.parse(data.toString());
      
      // Handle different message types
      if (msg.type === "handshake") {
        // Add new peers from handshake
        if (msg.knownPeers) {
          msg.knownPeers.forEach(peer => {
            if (peer !== `${getLocalIP()}:${PORT}` && 
                peer !== `${host}:${port}` &&
                !KNOWN_PEERS.has(peer)) {
              KNOWN_PEERS.add(peer);
              // Connect to new peer after delay
              setTimeout(() => connectToPeer(peer), 2000);
            }
          });
        }
        saveKnownPeers();
      }
      
      else if (msg.type === "discovery") {
        // Respond with our info
        const response = {
          type: "discovery-response",
          userId: USER_ID,
          port: PORT,
          address: `${getLocalIP()}:${PORT}`
        };
        socket.write(JSON.stringify(response));
      }
      
      else if (msg.type === "discovery-response") {
        // Add discovered peer
        if (msg.address && !KNOWN_PEERS.has(msg.address)) {
          KNOWN_PEERS.add(msg.address);
          saveKnownPeers();
          setTimeout(() => connectToPeer(msg.address), 1000);
        }
      }
      
      else if (msg.type === "chat") {
        // Normal chat message
        if (seen.has(msg.id)) return;
        seen.add(msg.id);
        
        if (msg.room !== currentRoom) return;
        
        const text = decrypt(msg.text, roomPassword);
        if (text === null) return;
        
        console.log(`\n💬 [${msg.room}] ${msg.username}: ${text}`);
        console.log("> ");
        
        broadcast(msg, socket);
      }
      
      else if (msg.type === "ping") {
        socket.write(JSON.stringify({ type: "pong", timestamp: Date.now() }));
      }
      
    } catch(e) {
      // Not JSON or error
    }
  });
  
  socket.on("error", (err) => {
    console.log(`❌ Connection error to ${address}: ${err.message}`);
  });
  
  socket.on("close", () => {
    console.log(`🔌 Disconnected from ${address}`);
    sockets.delete(socket);
    
    // Auto-reconnect for seed nodes
    const info = sockets.get(socket);
    if (info && (info.isSeed || KNOWN_PEERS.has(address))) {
      scheduleReconnect(address, info.retryCount + 1);
    }
  });
}

function scheduleReconnect(address, retryCount) {
  if (retryCount > MAX_RETRY) {
    console.log(`❌ Max retry reached for ${address}`);
    return;
  }
  
  const delay = RETRY_DELAY * Math.pow(2, retryCount - 1);
  console.log(`🔄 Reconnecting to ${address} in ${delay/1000}s (attempt ${retryCount}/${MAX_RETRY})`);
  
  const timer = setTimeout(() => {
    connectToPeer(address);
  }, delay);
  
  reconnectTimers.set(address, timer);
}

function saveKnownPeers() {
  const data = JSON.parse(fs.readFileSync("chat.json"));
  data.knownPeers = Array.from(KNOWN_PEERS);
  fs.writeFileSync("chat.json", JSON.stringify(data, null, 2));
}

// =====================
// 🖥️ Server
// =====================
const server = net.createServer((socket) => {
  const clientAddress = `${socket.remoteAddress}:${socket.remotePort}`;
  sockets.set(socket, { address: clientAddress, retryCount: 0, isSeed: false });
  
  socket.on("data", (data) => {
    try {
      const msg = JSON.parse(data.toString());
      
      if (msg.type === "handshake") {
        // Add to known peers
        if (!KNOWN_PEERS.has(clientAddress)) {
          KNOWN_PEERS.add(clientAddress);
          saveKnownPeers();
        }
        
        // Share our known peers
        const response = {
          type: "handshake",
          userId: USER_ID,
          username: USERNAME,
          knownPeers: Array.from(KNOWN_PEERS)
        };
        socket.write(JSON.stringify(response));
      }
      
      else if (msg.type === "chat") {
        if (seen.has(msg.id)) return;
        seen.add(msg.id);
        
        if (msg.room !== currentRoom) return;
        
        const text = decrypt(msg.text, roomPassword);
        if (text === null) return;
        
        console.log(`\n💬 [${msg.room}] ${msg.username}: ${text}`);
        console.log("> ");
        
        broadcast(msg, socket);
      }
      
      else if (msg.type === "ping") {
        socket.write(JSON.stringify({ type: "pong", timestamp: Date.now() }));
      }
      
      // Handle other message types...
      else if (msg.type === "discovery-response") {
        if (msg.address && !KNOWN_PEERS.has(msg.address)) {
          KNOWN_PEERS.add(msg.address);
          saveKnownPeers();
          setTimeout(() => connectToPeer(msg.address), 1000);
        }
      }
      
    } catch(e) {}
  });
  
  socket.on("close", () => {
    sockets.delete(socket);
  });
});

// =====================
// 📡 Broadcast with type
// =====================
function broadcast(msg, exclude = null) {
  const chatMsg = { ...msg, type: "chat" };
  sockets.forEach((_, socket) => {
    if (socket !== exclude) {
      try {
        socket.write(JSON.stringify(chatMsg));
      } catch(e) {}
    }
  });
}

// =====================
// 🛠️ Utility Functions
// =====================
function getLocalIP() {
  const interfaces = os.networkInterfaces();
  for (let name of Object.keys(interfaces)) {
    for (let iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return 'localhost';
}

function showNetworkInfo() {
  console.log("\n📡 Network Info:");
  console.log(`   Local IP: ${getLocalIP()}`);
  console.log(`   Port: ${PORT}`);
  console.log(`   Address: ${getLocalIP()}:${PORT}`);
  console.log(`   Connected Peers: ${sockets.size}`);
  console.log(`   Known Peers: ${KNOWN_PEERS.size}\n`);
}

function listPeers() {
  console.log("\n🌐 Connected Peers:");
  if (sockets.size === 0) {
    console.log("   No connected peers");
  } else {
    sockets.forEach((info, socket) => {
      console.log(`   • ${info.address}`);
    });
  }
  console.log("");
}

// =====================
// ⌨️ CLI with Enhanced Commands
// =====================
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

console.log("\n✨ Enhanced P2P Chat Network ✨");
console.log("=".repeat(40));
console.log("Commands:");
console.log("  /room <name>        → Join/Create room");
console.log("  /name <newname>     → Change username");
console.log("  /id                 → Show your ID");
console.log("  /peers              → List connected peers");
console.log("  /discover           → Discover new peers");
console.log("  /network            → Show network info");
console.log("  /connect <host:port> → Manually connect to peer");
console.log("  /save               → Save current peers to config");
console.log("  /clear              → Clear screen");
console.log("  /help               → Show this help");
console.log("  /exit               → Exit application");
console.log("=".repeat(40));

// Periodic peer discovery
setInterval(() => {
  if (sockets.size > 0) {
    discoverPeers();
  }
}, 30000); // Every 30 seconds

// Keep connection alive with ping
setInterval(() => {
  const pingMsg = { type: "ping", timestamp: Date.now() };
  sockets.forEach((_, socket) => {
    try {
      socket.write(JSON.stringify(pingMsg));
    } catch(e) {}
  });
}, 15000); // Every 15 seconds

rl.on("line", (input) => {
  // Change name
  if (input.startsWith("/name ")) {
    USERNAME = input.split(" ")[1];
    const data = JSON.parse(fs.readFileSync("chat.json"));
    data.username = USERNAME;
    fs.writeFileSync("chat.json", JSON.stringify(data, null, 2));
    console.log("✅ Username updated");
  }
  
  // Join room with password
  else if (input.startsWith("/room ")) {
    const roomName = input.split(" ")[1];
    rl.question("🔐 Enter password: ", (pass) => {
      currentRoom = roomName;
      roomPassword = pass;
      console.log(`✅ Joined room: ${roomName}`);
      rl.prompt();
    });
  }
  
  // Show ID
  else if (input === "/id") {
    console.log(`🆔 User ID: ${USER_ID}`);
  }
  
  // List peers
  else if (input === "/peers") {
    listPeers();
  }
  
  // Discover peers
  else if (input === "/discover") {
    discoverPeers();
  }
  
  // Network info
  else if (input === "/network") {
    showNetworkInfo();
  }
  
  // Manual connect
  else if (input.startsWith("/connect ")) {
    const peer = input.split(" ")[1];
    if (peer) {
      KNOWN_PEERS.add(peer);
      saveKnownPeers();
      connectToPeer(peer);
    }
  }
  
  // Save peers
  else if (input === "/save") {
    saveKnownPeers();
    console.log("✅ Peers saved to config");
  }
  
  // Clear screen
  else if (input === "/clear") {
    console.clear();
  }
  
  // Help
  else if (input === "/help") {
    console.log("\n📚 Commands:");
    console.log("  /room <name>        → Join/Create room");
    console.log("  /name <newname>     → Change username");
    console.log("  /id                 → Show your ID");
    console.log("  /peers              → List connected peers");
    console.log("  /discover           → Discover new peers");
    console.log("  /network            → Show network info");
    console.log("  /connect <host:port> → Manually connect to peer");
    console.log("  /save               → Save current peers to config");
    console.log("  /clear              → Clear screen");
    console.log("  /help               → Show this help");
    console.log("  /exit               → Exit application\n");
  }
  
  // Exit
  else if (input === "/exit") {
    console.log("👋 Goodbye!");
    process.exit(0);
  }
  
  // Send chat message
  else if (input.trim()) {
    const msg = {
      id: Math.random().toString(36).slice(2),
      userId: USER_ID,
      username: USERNAME,
      room: currentRoom,
      text: encrypt(input, roomPassword),
      type: "chat",
      timestamp: Date.now()
    };
    
    seen.add(msg.id);
    broadcast(msg);
    console.log("> ");
  }
});

// Start server and connect to seed nodes
server.listen(PORT, "0.0.0.0", () => {
  console.log(`\n🚀 Server running on port ${PORT}`);
  console.log(`👤 You are: ${USERNAME} (${USER_ID})`);
  console.log(`💬 Current room: ${currentRoom}`);
  
  // Connect to seed nodes
  if (SEED_NODES.length > 0) {
    console.log(`\n🌱 Connecting to ${SEED_NODES.length} seed nodes...`);
    SEED_NODES.forEach(connectToPeer);
  } else {
    console.log("\n💡 No seed nodes configured. Use /connect to add peers");
    console.log("   Or set SEED_NODES environment variable:");
    console.log("   SEED_NODES=\"192.168.1.100:3001,192.168.1.101:3001\" node server.js\n");
  }
});

// Handle graceful shutdown
process.on("SIGINT", () => {
  console.log("\n\n👋 Saving state and exiting...");
  saveKnownPeers();
  process.exit(0);
});
