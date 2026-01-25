import { ImageResponse } from 'next/og';

export const runtime = 'edge';

export const size = {
  width: 64,
  height: 64,
};

export const contentType = 'image/png';

export default function Icon() {
  return new ImageResponse(
    (
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          background: '#0f172a',
          color: '#fbbf24',
          width: '100%',
          height: '100%',
          fontSize: 32,
          fontWeight: 700,
          letterSpacing: -1,
        }}
      >
        RT
      </div>
    ),
    {
      ...size,
    },
  );
}
