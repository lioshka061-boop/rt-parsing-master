import { NextResponse } from 'next/server';

const apiKey = process.env.NOVAPOSHTA_API_KEY;
const endpoint = 'https://api.novaposhta.ua/v2.0/json/';

export async function GET(request: Request) {
  if (!apiKey) {
    return NextResponse.json({ ok: false, error: 'missing_api_key' }, { status: 500 });
  }

  const url = new URL(request.url);
  const cityRef = url.searchParams.get('cityRef')?.trim() || '';
  if (!cityRef) {
    return NextResponse.json({ ok: true, data: [] });
  }

  const payload = {
    apiKey,
    modelName: 'Address',
    calledMethod: 'getWarehouses',
    methodProperties: {
      CityRef: cityRef,
      Limit: 200,
    },
  };

  try {
    const res = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!data?.success) {
      return NextResponse.json({ ok: false, error: 'api_error', details: data?.errors || [] });
    }
    const items = (data.data || []).map((item: any) => ({
      Ref: item.Ref,
      Description: item.Description,
    }));
    return NextResponse.json({ ok: true, data: items });
  } catch {
    return NextResponse.json({ ok: false, error: 'network_error' }, { status: 502 });
  }
}
