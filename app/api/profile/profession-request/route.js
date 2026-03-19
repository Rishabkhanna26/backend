import {
  GET as getBusinessTypeRequest,
  POST as postBusinessTypeRequest,
} from '../business-type-request/route';

export async function GET(request) {
  return getBusinessTypeRequest(request);
}

export async function POST(request) {
  return postBusinessTypeRequest(request);
}
