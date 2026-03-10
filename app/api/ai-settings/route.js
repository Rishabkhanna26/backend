import { requireAuth } from '../../../lib/auth-server';
import { getAdminAISettings, updateAdminAISettings } from '../../../lib/db-helpers';

export const runtime = 'nodejs';

const DEFAULTS = Object.freeze({
  ai_enabled: false,
  ai_prompt: '',
  ai_blocklist: '',
  automation_enabled: true,
  automation_trigger_mode: 'any',
  automation_trigger_keyword: '',
  whatsapp_pending_recovery_enabled: true,
  whatsapp_only_post_config_messages: true,
  whatsapp_preconfig_message_grace_ms: 30000,
  whatsapp_recovery_window_hours: 24,
  whatsapp_recovery_batch_limit: 20,
  whatsapp_recovery_analysis_message_limit: 18,
  whatsapp_recovery_ai_required: true,
  whatsapp_recovery_ai_model: '',
  appointment_start_hour: 9,
  appointment_end_hour: 20,
  appointment_slot_minutes: 60,
  appointment_window_months: 3,
});

const parseBoundedInteger = (value, { min, max }) => {
  if (value === undefined) return { provided: false, value: undefined };
  const num = Number(value);
  if (!Number.isInteger(num) || num < min || num > max) {
    return { provided: true, error: `Must be an integer between ${min} and ${max}.` };
  }
  return { provided: true, value: num };
};

export async function GET() {
  try {
    const user = await requireAuth();
    const settings = await getAdminAISettings(user.id);
    return Response.json({
      success: true,
      data: settings || DEFAULTS,
    });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}

export async function PUT(request) {
  try {
    const user = await requireAuth();
    const body = await request.json();
    const ai_enabled = typeof body.ai_enabled === 'boolean' ? body.ai_enabled : undefined;
    const ai_prompt = typeof body.ai_prompt === 'string' ? body.ai_prompt : undefined;
    const ai_blocklist = typeof body.ai_blocklist === 'string' ? body.ai_blocklist : undefined;
    const automation_enabled =
      typeof body.automation_enabled === 'boolean' ? body.automation_enabled : undefined;
    const automation_trigger_mode =
      typeof body.automation_trigger_mode === 'string'
        ? body.automation_trigger_mode.trim().toLowerCase()
        : undefined;
    const automation_trigger_keyword =
      typeof body.automation_trigger_keyword === 'string'
        ? body.automation_trigger_keyword.trim().slice(0, 40)
        : undefined;
    if (automation_trigger_mode === 'keyword') {
      const keyword = (automation_trigger_keyword || '').trim();
      if (!keyword || keyword.length < 3) {
        return Response.json(
          { success: false, error: 'Automation keyword must be at least 3 characters.' },
          { status: 400 }
        );
      }
    }
    const whatsapp_pending_recovery_enabled =
      typeof body.whatsapp_pending_recovery_enabled === 'boolean'
        ? body.whatsapp_pending_recovery_enabled
        : undefined;
    const whatsapp_only_post_config_messages =
      typeof body.whatsapp_only_post_config_messages === 'boolean'
        ? body.whatsapp_only_post_config_messages
        : undefined;
    const whatsapp_recovery_ai_required =
      typeof body.whatsapp_recovery_ai_required === 'boolean'
        ? body.whatsapp_recovery_ai_required
        : undefined;
    const whatsapp_recovery_ai_model =
      typeof body.whatsapp_recovery_ai_model === 'string'
        ? body.whatsapp_recovery_ai_model.trim().slice(0, 120)
        : undefined;
    const startHour = parseBoundedInteger(body.appointment_start_hour, { min: 0, max: 23 });
    if (startHour.error) {
      return Response.json(
        { success: false, error: `appointment_start_hour: ${startHour.error}` },
        { status: 400 }
      );
    }
    const endHour = parseBoundedInteger(body.appointment_end_hour, { min: 1, max: 24 });
    if (endHour.error) {
      return Response.json(
        { success: false, error: `appointment_end_hour: ${endHour.error}` },
        { status: 400 }
      );
    }
    const slotMinutes = parseBoundedInteger(body.appointment_slot_minutes, { min: 15, max: 240 });
    if (slotMinutes.error) {
      return Response.json(
        { success: false, error: `appointment_slot_minutes: ${slotMinutes.error}` },
        { status: 400 }
      );
    }
    const windowMonths = parseBoundedInteger(body.appointment_window_months, { min: 1, max: 24 });
    if (windowMonths.error) {
      return Response.json(
        { success: false, error: `appointment_window_months: ${windowMonths.error}` },
        { status: 400 }
      );
    }
    const preconfigGraceMs = parseBoundedInteger(body.whatsapp_preconfig_message_grace_ms, {
      min: 0,
      max: 300000,
    });
    if (preconfigGraceMs.error) {
      return Response.json(
        { success: false, error: `whatsapp_preconfig_message_grace_ms: ${preconfigGraceMs.error}` },
        { status: 400 }
      );
    }
    const recoveryWindowHours = parseBoundedInteger(body.whatsapp_recovery_window_hours, {
      min: 1,
      max: 168,
    });
    if (recoveryWindowHours.error) {
      return Response.json(
        { success: false, error: `whatsapp_recovery_window_hours: ${recoveryWindowHours.error}` },
        { status: 400 }
      );
    }
    const recoveryBatchLimit = parseBoundedInteger(body.whatsapp_recovery_batch_limit, {
      min: 1,
      max: 200,
    });
    if (recoveryBatchLimit.error) {
      return Response.json(
        { success: false, error: `whatsapp_recovery_batch_limit: ${recoveryBatchLimit.error}` },
        { status: 400 }
      );
    }
    const recoveryAnalysisLimit = parseBoundedInteger(
      body.whatsapp_recovery_analysis_message_limit,
      { min: 6, max: 80 }
    );
    if (recoveryAnalysisLimit.error) {
      return Response.json(
        {
          success: false,
          error: `whatsapp_recovery_analysis_message_limit: ${recoveryAnalysisLimit.error}`,
        },
        { status: 400 }
      );
    }

    const finalStartHour =
      startHour.provided && startHour.value !== undefined ? startHour.value : undefined;
    const finalEndHour = endHour.provided && endHour.value !== undefined ? endHour.value : undefined;
    if (
      finalStartHour !== undefined &&
      finalEndHour !== undefined &&
      finalEndHour <= finalStartHour
    ) {
      return Response.json(
        {
          success: false,
          error: 'appointment_end_hour must be greater than appointment_start_hour.',
        },
        { status: 400 }
      );
    }

    const updated = await updateAdminAISettings(user.id, {
      ai_enabled,
      ai_prompt,
      ai_blocklist,
      automation_enabled,
      automation_trigger_mode:
        automation_trigger_mode === 'keyword' ? 'keyword' : automation_trigger_mode === 'any' ? 'any' : undefined,
      automation_trigger_keyword: automation_trigger_keyword !== undefined ? automation_trigger_keyword : undefined,
      whatsapp_pending_recovery_enabled,
      whatsapp_only_post_config_messages,
      whatsapp_preconfig_message_grace_ms:
        preconfigGraceMs.provided && preconfigGraceMs.value !== undefined
          ? preconfigGraceMs.value
          : undefined,
      whatsapp_recovery_window_hours:
        recoveryWindowHours.provided && recoveryWindowHours.value !== undefined
          ? recoveryWindowHours.value
          : undefined,
      whatsapp_recovery_batch_limit:
        recoveryBatchLimit.provided && recoveryBatchLimit.value !== undefined
          ? recoveryBatchLimit.value
          : undefined,
      whatsapp_recovery_analysis_message_limit:
        recoveryAnalysisLimit.provided && recoveryAnalysisLimit.value !== undefined
          ? recoveryAnalysisLimit.value
          : undefined,
      whatsapp_recovery_ai_required,
      whatsapp_recovery_ai_model:
        whatsapp_recovery_ai_model !== undefined ? whatsapp_recovery_ai_model : undefined,
      appointment_start_hour: finalStartHour,
      appointment_end_hour: finalEndHour,
      appointment_slot_minutes:
        slotMinutes.provided && slotMinutes.value !== undefined ? slotMinutes.value : undefined,
      appointment_window_months:
        windowMonths.provided && windowMonths.value !== undefined ? windowMonths.value : undefined,
    });
    return Response.json({ success: true, data: updated });
  } catch (error) {
    if (error.status === 401) {
      return Response.json({ success: false, error: 'Unauthorized' }, { status: 401 });
    }
    return Response.json({ success: false, error: error.message }, { status: 500 });
  }
}
