import { requireReadAuth } from '../../../../lib/auth-server';
import { getReportOverview } from '../../../../lib/db-helpers';
import { sanitizeText } from '../../../../lib/sanitize.js';
import PDFDocument from 'pdfkit';
import * as XLSX from 'xlsx';

export const runtime = 'nodejs';

function resolveRange(range) {
  const now = new Date();
  let days = 7;
  if (range === '30days') days = 30;
  if (range === '90days') days = 90;
  if (range === '1year') days = 365;
  const start = new Date(now);
  start.setDate(start.getDate() - days);
  return start.toISOString().slice(0, 10);
}

const toDateLabel = (value) => {
  if (!value) return '';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return '';
  return d.toISOString().slice(0, 10);
};

const buildXlsx = ({ overview, range, scopeLabel }) => {
  const wb = XLSX.utils.book_new();

  const summaryRows = [
    { Metric: 'Range', Value: range },
    { Metric: 'Scope', Value: scopeLabel },
    { Metric: 'Generated At', Value: new Date().toISOString() },
    { Metric: 'Total Messages (Incoming)', Value: Number(overview?.totalMessages || 0) },
    { Metric: 'Total Leads/Contacts', Value: Number(overview?.totalContacts || 0) },
  ];
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(summaryRows), 'Summary');

  const messageRows = (overview?.messageStats || []).map((row) => ({
    Date: toDateLabel(row?.date),
    Messages: Number(row?.count || 0),
  }));
  XLSX.utils.book_append_sheet(
    wb,
    XLSX.utils.json_to_sheet(messageRows.length ? messageRows : [{ Date: '', Messages: 0 }]),
    'Messages'
  );

  const leadRows = (overview?.leadStats || []).map((row) => ({
    Status: sanitizeText(row?.label || row?.status || '', 60) || '',
    Count: Number(row?.count || 0),
  }));
  XLSX.utils.book_append_sheet(
    wb,
    XLSX.utils.json_to_sheet(leadRows.length ? leadRows : [{ Status: '', Count: 0 }]),
    'Leads'
  );

  const agentRows = (overview?.agentPerformance || []).map((row) => ({
    AdminId: Number(row?.id || 0),
    Name: sanitizeText(row?.name || '', 80) || '',
    Tier: sanitizeText(row?.admin_tier || '', 40) || '',
    Status: sanitizeText(row?.status || '', 40) || '',
    MessagesSent: Number(row?.messages_sent || 0),
    ActiveChats7d: Number(row?.active_chats || 0),
  }));
  XLSX.utils.book_append_sheet(
    wb,
    XLSX.utils.json_to_sheet(agentRows.length ? agentRows : [{ AdminId: 0, Name: '', Tier: '', Status: '', MessagesSent: 0, ActiveChats7d: 0 }]),
    'Agents'
  );

  const campaignRows = (overview?.topCampaigns || []).map((row) => ({
    CampaignId: Number(row?.id || 0),
    Title: sanitizeText(row?.title || '', 120) || '',
    Status: sanitizeText(row?.status || '', 40) || '',
    Sent: Number(row?.sent_count || 0),
    Delivered: Number(row?.delivered_count || 0),
    CreatedAt: toDateLabel(row?.created_at),
  }));
  XLSX.utils.book_append_sheet(
    wb,
    XLSX.utils.json_to_sheet(campaignRows.length ? campaignRows : [{ CampaignId: 0, Title: '', Status: '', Sent: 0, Delivered: 0, CreatedAt: '' }]),
    'Campaigns'
  );

  return XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
};

const buildPdf = async ({ overview, range, scopeLabel }) => {
  const doc = new PDFDocument({ size: 'A4', margin: 48 });
  const chunks = [];

  const pdfBuffer = await new Promise((resolve, reject) => {
    doc.on('data', (chunk) => chunks.push(chunk));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);

    doc.fontSize(18).text('AlgoChat Report', { align: 'left' });
    doc.moveDown(0.5);
    doc.fontSize(10).fillColor('#444').text(`Range: ${range}`);
    doc.text(`Scope: ${scopeLabel}`);
    doc.text(`Generated: ${new Date().toISOString()}`);
    doc.moveDown(1);

    doc.fillColor('#111').fontSize(12).text('Summary');
    doc.moveDown(0.25);
    doc.fontSize(10).fillColor('#222').text(`Total incoming messages: ${Number(overview?.totalMessages || 0)}`);
    doc.text(`Total leads/contacts: ${Number(overview?.totalContacts || 0)}`);
    doc.moveDown(0.75);

    doc.fillColor('#111').fontSize(12).text('Leads by Status');
    doc.moveDown(0.25);
    (overview?.leadStats || []).forEach((row) => {
      doc.fontSize(10).fillColor('#222').text(
        `${sanitizeText(row?.label || row?.status || '', 60) || ''}: ${Number(row?.count || 0)}`
      );
    });

    doc.end();
  });

  return pdfBuffer;
};

export async function GET(request) {
  try {
    const authUser = await requireReadAuth();
    const { searchParams } = new URL(request.url);
    const range = searchParams.get('range') || '7days';
    const format = String(searchParams.get('format') || 'xlsx').trim().toLowerCase();

    if (!['xlsx', 'pdf'].includes(format)) {
      return Response.json({ success: false, error: 'Invalid format. Use xlsx or pdf.' }, { status: 400 });
    }

    const startDate = resolveRange(range);
    const adminScopeId = authUser.admin_tier === 'super_admin' ? null : authUser.id;
    const scopeLabel = adminScopeId ? `admin:${authUser.id}` : 'all';

    const overview = await getReportOverview(startDate, adminScopeId);
    const safeRange = sanitizeText(range, 20).replace(/[^a-z0-9]+/gi, '-').toLowerCase() || 'range';
    const filenameBase = `algochat-report-${safeRange}-${scopeLabel}`.replace(/[^a-z0-9._-]+/gi, '-');

    if (format === 'pdf') {
      const pdf = await buildPdf({ overview, range, scopeLabel });
      return new Response(pdf, {
        headers: {
          'Content-Type': 'application/pdf',
          'Content-Disposition': `attachment; filename="${filenameBase}.pdf"`,
          'Cache-Control': 'no-store',
        },
      });
    }

    const xlsx = buildXlsx({ overview, range, scopeLabel });
    return new Response(xlsx, {
      headers: {
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'Content-Disposition': `attachment; filename="${filenameBase}.xlsx"`,
        'Cache-Control': 'no-store',
      },
    });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}
