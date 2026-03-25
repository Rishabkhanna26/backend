import { requireReadAuth } from '../../../../lib/auth-server';
import { getConnection } from '../../../../lib/db-helpers';
import PDFDocument from 'pdfkit/js/pdfkit.standalone.js';

export const runtime = 'nodejs';

const toNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const safeText = (value, fallback = '-') => {
  const text = String(value ?? '').trim();
  if (!text) return fallback;
  return text.slice(0, 140);
};

const formatInr = (value) => {
  const amount = toNumber(value, 0);
  return `Rs ${amount.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
};

const formatDate = (value) => {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '-';
  return date.toLocaleDateString('en-IN', {
    day: '2-digit',
    month: 'long',
    year: 'numeric',
  });
};

const formatDateTime = (value) => {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '-';
  return date.toLocaleString('en-IN');
};

const buildPurposeLabel = (purchase) => {
  const purpose = String(purchase?.purpose || '').toLowerCase();
  if (purpose === 'dashboard') {
    const months = Math.max(1, Math.trunc(Number(purchase?.subscription_months) || 1));
    return `Dashboard Subscription (${months} month${months > 1 ? 's' : ''})`;
  }
  if (purpose === 'business_type_change') return 'Business Type Change';
  if (purpose === 'prepaid') return 'Prepaid Tokens';
  return 'Pay-as-you-go';
};

const drawLabelValue = (doc, { x, y, label, value, width }) => {
  doc
    .font('Helvetica')
    .fontSize(9)
    .fillColor('#6B7280')
    .text(label, x, y, { width });
  doc
    .font('Helvetica-Bold')
    .fontSize(11)
    .fillColor('#111827')
    .text(value, x, y + 12, { width });
};

const buildInvoicePdf = async ({ purchase }) => {
  const doc = new PDFDocument({ size: 'A4', margin: 40 });
  const chunks = [];

  const total = toNumber(purchase?.amount, 0);
  const baseAmount = toNumber(purchase?.base_amount, total);
  const maintenanceFee = toNumber(purchase?.maintenance_fee, 0);
  const paidAmount = toNumber(purchase?.paid_amount, total);
  const dueAmount = Math.max(Number((total - paidAmount).toFixed(2)), 0);
  const discountPct = toNumber(purchase?.discount_pct, 0);
  const safeDiscountPct = discountPct > 0 && discountPct < 100 ? discountPct : 0;
  const grossBase = safeDiscountPct
    ? Number((baseAmount / (1 - safeDiscountPct / 100)).toFixed(2))
    : baseAmount;
  const grossTotal = Number((grossBase + maintenanceFee).toFixed(2));

  const invoiceNo = `INV-${safeText(purchase?.id, Date.now())}`;
  const invoiceDate = formatDate(purchase?.paid_at || purchase?.created_at);
  const paymentDateTime = formatDateTime(purchase?.paid_at || purchase?.created_at);
  const paymentMethod = 'Razorpay';

  const contentLeft = 42;
  const contentRight = doc.page.width - 42;
  const contentWidth = contentRight - contentLeft;
  const halfWidth = (contentWidth - 28) / 2;

  const pdfBuffer = await new Promise((resolve, reject) => {
    doc.on('data', (chunk) => chunks.push(chunk));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);

    doc
      .font('Helvetica-Bold')
      .fontSize(32)
      .fillColor('#0F2742')
      .text('INVOICE', contentLeft, 56);
    doc
      .moveTo(contentLeft, 90)
      .lineTo(contentRight, 90)
      .lineWidth(1.5)
      .strokeColor('#FE8802')
      .stroke();

    const fromToY = 104;
    doc.font('Helvetica-Bold').fontSize(12).fillColor('#111827').text('From:', contentLeft, fromToY);
    doc.font('Helvetica').fontSize(10.5).fillColor('#1F2937');
    doc.text('Algo Aura', contentLeft, fromToY + 16);
    doc.text('Web Development & Automation Agency', contentLeft, fromToY + 31);
    doc.text('Jalandhar', contentLeft, fromToY + 46);
    doc.text('Email: teamalgoaura@gmail.com', contentLeft, fromToY + 61);
    doc.text('Phone: +91 8708767499', contentLeft, fromToY + 76);

    const toX = contentLeft + halfWidth + 28;
    doc.font('Helvetica-Bold').fontSize(12).fillColor('#111827').text('To:', toX, fromToY);
    doc.font('Helvetica').fontSize(10.5).fillColor('#1F2937');
    doc.text(safeText(purchase?.admin_name || `Admin #${purchase?.admin_id}`), toX, fromToY + 16, { width: halfWidth });
    doc.text(`Email: ${safeText(purchase?.admin_email)}`, toX, fromToY + 31, { width: halfWidth });
    doc.text(`Phone: ${safeText(purchase?.admin_phone)}`, toX, fromToY + 46, { width: halfWidth });

    const detailsY = 214;
    doc
      .font('Helvetica-Bold')
      .fontSize(13)
      .fillColor('#FE8802')
      .text('Invoice Details', contentLeft, detailsY);
    doc
      .font('Helvetica')
      .fontSize(10.5)
      .fillColor('#111827')
      .text(`Invoice No: ${invoiceNo}`, contentLeft, detailsY + 18);
    doc.text(`Invoice Date: ${invoiceDate}`, contentLeft, detailsY + 33);
    doc.text(`Payment Method: ${paymentMethod}`, contentLeft, detailsY + 48);

    doc
      .moveTo(0, 290)
      .lineTo(doc.page.width, 290)
      .lineWidth(1.2)
      .strokeColor('#0F2742')
      .stroke();

    const tableTop = 316;
    const descWidth = 250;
    const qtyWidth = 80;
    const priceWidth = 90;
    const amountWidth = contentWidth - descWidth - qtyWidth - priceWidth;
    const col = {
      desc: contentLeft,
      qty: contentLeft + descWidth,
      price: contentLeft + descWidth + qtyWidth,
      amount: contentLeft + descWidth + qtyWidth + priceWidth,
    };
    doc.rect(contentLeft, tableTop, contentWidth, 34).fill('#ECECEC');
    doc.font('Helvetica-Bold').fontSize(11).fillColor('#FE8802');
    doc.text('Description', col.desc + 8, tableTop + 12, { width: descWidth - 16 });
    doc.text('Qty', col.qty + 8, tableTop + 12, { width: qtyWidth - 16, align: 'center' });
    doc.text('Price', col.price + 8, tableTop + 12, { width: priceWidth - 16, align: 'right' });
    doc.text('Amount', col.amount + 8, tableTop + 12, { width: amountWidth - 16, align: 'right' });

    const rows = [
      {
        description: buildPurposeLabel(purchase),
        qty: '1',
        price: formatInr(baseAmount),
        amount: formatInr(baseAmount),
      },
    ];
    if (maintenanceFee > 0) {
      rows.push({
        description: 'Maintenance Fee (12%)',
        qty: '1',
        price: formatInr(maintenanceFee),
        amount: formatInr(maintenanceFee),
        });
      }

    let rowY = tableTop + 34;
    rows.forEach((row, index) => {
      const rowHeight = 42;
      doc.rect(contentLeft, rowY, contentWidth, rowHeight).fill('#FFFFFF');
      doc.font('Helvetica').fontSize(10.5).fillColor('#111827');
      doc.text(safeText(row.description), col.desc + 8, rowY + 14, { width: descWidth - 16 });
      doc.text(row.qty, col.qty + 8, rowY + 14, { width: qtyWidth - 16, align: 'center' });
      doc.text(row.price, col.price + 8, rowY + 14, { width: priceWidth - 16, align: 'right' });
      doc.text(row.amount, col.amount + 8, rowY + 14, { width: amountWidth - 16, align: 'right' });
      rowY += rowHeight;
      doc
        .moveTo(contentLeft, rowY)
        .lineTo(contentRight, rowY)
        .lineWidth(1)
        .strokeColor('#7E8CA0')
        .stroke();
    });

    const totalsY = rowY + 14;
    const labelX = contentRight - 192;
    const valueX = contentRight - 92;
    doc.font('Helvetica-Bold').fontSize(13).fillColor('#111827');
    doc.text('Total', labelX, totalsY, { width: 88, align: 'right' });
    doc.text(formatInr(grossTotal), valueX, totalsY, { width: 88, align: 'right' });
    doc.text('Offer Price', labelX, totalsY + 22, { width: 88, align: 'right' });
    doc.text(formatInr(total), valueX, totalsY + 22, { width: 88, align: 'right' });
    doc.text('Paid', labelX, totalsY + 44, { width: 88, align: 'right' });
    doc.text(formatInr(paidAmount), valueX, totalsY + 44, { width: 88, align: 'right' });
    doc.text('Due', labelX, totalsY + 66, { width: 88, align: 'right' });
    doc.text(formatInr(dueAmount), valueX, totalsY + 66, { width: 88, align: 'right' });

    const footerY = totalsY + 108;
    doc
      .moveTo(0, footerY)
      .lineTo(doc.page.width, footerY)
      .lineWidth(1.2)
      .strokeColor('#0F2742')
      .stroke();
    doc.font('Helvetica-Bold').fontSize(34).fillColor('#FE8802').text('Algo Aura', contentLeft, footerY + 12);
    doc.font('Helvetica').fontSize(17).fillColor('#0F2742').text('Your Vision, Our Mission', contentLeft, footerY + 50);

    doc.font('Helvetica-Bold').fontSize(12).fillColor('#0F2742');
    doc.text('Contact', contentRight - 120, footerY + 18, { width: 120, align: 'right' });
    doc.font('Helvetica').fontSize(10).fillColor('#1F3A5F');
    doc.text('+91 87087 67499', contentRight - 120, footerY + 36, { width: 120, align: 'right' });
    doc.text('teamalgoaura@gmail.com', contentRight - 150, footerY + 52, { width: 150, align: 'right' });

    doc.end();
  });

  return pdfBuffer;
};

export async function GET(request) {
  try {
    const user = await requireReadAuth();
    const { searchParams } = new URL(request.url);
    const purchaseId = Number(searchParams.get('purchase_id'));
    if (!Number.isInteger(purchaseId) || purchaseId <= 0) {
      return Response.json(
        { success: false, error: 'purchase_id is required.' },
        { status: 400 }
      );
    }

    const connection = await getConnection();
    try {
      const [rows] = await connection.query(
        `
          SELECT
            p.id,
            p.admin_id,
            p.payment_link_id,
            p.amount,
            p.base_amount,
            p.maintenance_fee,
            p.currency,
            p.purpose,
            p.subscription_months,
            p.discount_pct,
            p.dashboard_monthly_amount,
            p.status,
            p.paid_amount,
            p.paid_at,
            p.created_at,
            a.name AS admin_name,
            a.email AS admin_email,
            a.phone AS admin_phone
          FROM admin_payment_links p
          JOIN admins a ON a.id = p.admin_id
          WHERE p.id = ?
          LIMIT 1
        `,
        [purchaseId]
      );
      const purchase = rows?.[0] || null;
      if (!purchase) {
        return Response.json(
          { success: false, error: 'Invoice record not found.' },
          { status: 404 }
        );
      }
      if (user.admin_tier !== 'super_admin' && Number(purchase.admin_id) !== Number(user.id)) {
        return Response.json({ success: false, error: 'Forbidden' }, { status: 403 });
      }

      const pdf = await buildInvoicePdf({ purchase });
      const filename = `invoice-${safeText(purchase.id, purchaseId)}.pdf`;
      return new Response(pdf, {
        headers: {
          'Content-Type': 'application/pdf',
          'Content-Disposition': `attachment; filename="${filename}"`,
          'Cache-Control': 'no-store',
        },
      });
    } finally {
      connection.release();
    }
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}
