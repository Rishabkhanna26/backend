import { checkSubscriptionStatus } from './auth-server';
import { NextResponse } from 'next/server';

export async function protectModificationAction(user, action = 'modify') {
  const status = checkSubscriptionStatus(user);
  
  if (!status.allowed && status.viewOnly) {
    return NextResponse.json(
      { 
        success: false, 
        error: status.message,
        readOnly: true,
        userMessage: `${status.message} You can view your data but cannot ${action} it until access is restored.`
      },
      { status: 403 }
    );
  }
  
  if (!status.allowed) {
    return NextResponse.json(
      { success: false, error: status.message },
      { status: status.status === 'unauthorized' ? 401 : 403 }
    );
  }
  
  return null; // Allowed
}

export function isModificationAllowed(user) {
  const status = checkSubscriptionStatus(user);
  return status.allowed;
}
