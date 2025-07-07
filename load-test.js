// load-test.js
import ws from 'k6/ws';
import { check } from 'k6';

// --- Test Configuration ---
export const options = {
  // Simulate 50 users (VUs) connecting and staying active
  // for a duration of 30 seconds.
  vus: 50,
  duration: '30s',
};

export default function () {
  // --- IMPORTANT: Replace with a valid token ---
  const token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI0IiwidXNlcl90eXBlIjoibGl2ZSIsImFjY291bnRfbnVtYmVyIjoiRFlZNUEiLCJleHAiOjE3NTE4NzI2NTksImlhdCI6MTc1MTg3MDg1OX0.6QUGIMocWo2oqDeZAHoOx7igPC4NFLGuB8dqVcehACs'; 
  const url = `ws://127.00.1:8000/api/v1/ws/market-data?token=${token}`;

  const res = ws.connect(url, null, function (socket) {
    // This function runs when the connection is established
    socket.on('open', () => console.log('WebSocket connection established!'));

    // Listen for messages from the server
    socket.on('message', (data) => {
      // You can log the message, but it might be too noisy for a load test
      // console.log('Message received: ', data);
    });

    // After 25 seconds, close the connection
    socket.setTimeout(function () {
      socket.close();
    }, 25000);

    socket.on('close', () => console.log('WebSocket connection closed.'));
  });

  // Check if the connection was successful
  check(res, { 'status is 101': (r) => r && r.status === 101 });
}