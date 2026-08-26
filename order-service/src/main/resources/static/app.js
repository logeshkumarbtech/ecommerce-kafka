const STEPS = [
  { key: 'CREATED', label: 'Order Created' },
  { key: 'STOCK_RESERVED', label: 'Stock Reserved' },
  { key: 'PAID', label: 'Payment Charged' },
  { key: 'SHIPPED', label: 'Shipped' }
];

const ordersContainer = document.getElementById('orders');
const orderForm = document.getElementById('order-form');
const toastContainer = document.getElementById('toast-container');

function stepIndex(status) {
  return STEPS.findIndex(s => s.key === status);
}

function renderOrder(order) {
  const currentIndex = stepIndex(order.status);
  let card = document.getElementById(`order-${order.orderId}`);

  if (!card) {
    card = document.createElement('div');
    card.id = `order-${order.orderId}`;
    card.className = 'order-card';
    ordersContainer.prepend(card);
  }

  const stepsHtml = STEPS.map((step, i) => {
    const state = i < currentIndex ? 'done' : i === currentIndex ? 'active' : '';
    const icon = i < currentIndex ? '✓' : i + 1;
    return `
      <div class="step ${state}">
        <div class="dot">${icon}</div>
        <div class="label">${step.label}</div>
      </div>`;
  }).join('');

  card.innerHTML = `
    <div class="meta">
      <span class="order-id">Order ${order.orderId.slice(0, 8)}</span>
      <span>${order.customerId || ''} • ${order.productId || ''} • qty ${order.quantity || ''} • $${order.amount || ''}</span>
    </div>
    <div class="steps">${stepsHtml}</div>
    ${order.trackingNumber ? `<div class="meta" style="margin-top:10px;margin-bottom:0"><span>Tracking: ${order.trackingNumber}</span><span>ETA: ${order.estimatedDelivery}</span></div>` : ''}
  `;
}

function showToast(message) {
  const toast = document.createElement('div');
  toast.className = 'toast';
  toast.textContent = message;
  toastContainer.appendChild(toast);
  setTimeout(() => toast.remove(), 4000);
}

function toastForStatus(order) {
  const messages = {
    CREATED: `Order ${order.orderId.slice(0, 8)} created`,
    STOCK_RESERVED: `Stock reserved for order ${order.orderId.slice(0, 8)}`,
    PAID: `Payment confirmed for order ${order.orderId.slice(0, 8)}`,
    SHIPPED: `Order ${order.orderId.slice(0, 8)} shipped — tracking ${order.trackingNumber}`
  };
  return messages[order.status] || `Order ${order.orderId.slice(0, 8)} updated`;
}

orderForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  const payload = {
    customerId: document.getElementById('customerId').value,
    productId: document.getElementById('productId').value,
    quantity: parseInt(document.getElementById('quantity').value, 10),
    amount: parseFloat(document.getElementById('amount').value)
  };

  const res = await fetch('/api/orders', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    showToast('Failed to place order');
  }
});

// Hydrate existing orders on page load
fetch('/api/orders')
  .then(res => res.json())
  .then(orders => orders.forEach(renderOrder))
  .catch(() => {});

// Live updates via Server-Sent Events
const eventSource = new EventSource('/api/orders/stream');
eventSource.addEventListener('order-update', (e) => {
  const order = JSON.parse(e.data);
  renderOrder(order);
  showToast(toastForStatus(order));
});
