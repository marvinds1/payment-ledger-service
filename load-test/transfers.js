import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const ACCOUNTS = Number(__ENV.ACCOUNTS || 20);

export const options = {
  scenarios: {
    transfers: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 200),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<200'],
  },
};

const json = { 'Content-Type': 'application/json' };

export function setup() {
  const ids = [];
  for (let i = 0; i < ACCOUNTS; i++) {
    const account = http.post(`${BASE_URL}/api/v1/accounts`,
      JSON.stringify({ ownerName: `load-${i}`, currency: 'IDR' }), { headers: json }).json();
    http.post(`${BASE_URL}/api/v1/accounts/${account.id}/deposits`,
      JSON.stringify({ amount: 1000000000 }), { headers: { ...json, 'Idempotency-Key': uuidv4() } });
    ids.push(account.id);
  }
  return { ids };
}

export default function ({ ids }) {
  const source = ids[Math.floor(Math.random() * ids.length)];
  let target = ids[Math.floor(Math.random() * ids.length)];
  if (target === source) {
    target = ids[(ids.indexOf(source) + 1) % ids.length];
  }
  const res = http.post(`${BASE_URL}/api/v1/transfers`,
    JSON.stringify({ sourceAccountId: source, targetAccountId: target, amount: 1000, currency: 'IDR' }),
    { headers: { ...json, 'Idempotency-Key': uuidv4() } });
  check(res, { 'transfer created': (r) => r.status === 201 });
}
