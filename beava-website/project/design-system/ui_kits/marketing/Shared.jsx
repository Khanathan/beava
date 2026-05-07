// ui_kits/marketing/Shared.jsx — small reusable bits
const Icon = ({ name, size = 20, stroke = 1.75, ...rest }) => {
  const paths = {
    arrow:   <><path d="M5 12h14"/><path d="M13 6l6 6-6 6"/></>,
    star:    <path d="M12 2l3.1 6.3 6.9 1-5 4.9 1.2 6.8L12 17.8 5.8 21l1.2-6.8-5-4.9 6.9-1z"/>,
    chart:   <><path d="M3 3v18h18"/><path d="M7 14l4-4 4 4 5-5"/></>,
    clock:   <><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></>,
    list:    <><path d="M4 6h16"/><path d="M4 12h10"/><path d="M4 18h6"/></>,
    gauge:   <><path d="M12 14l4-4"/><circle cx="12" cy="12" r="9"/></>,
    lock:    <><rect x="5" y="11" width="14" height="9" rx="2"/><path d="M8 11V8a4 4 0 0 1 8 0v3"/></>,
    zap:     <path d="M13 2L3 14h9l-1 8 10-12h-9z"/>,
    search:  <><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.35-4.35"/></>,
    github:  <path d="M12 2a10 10 0 0 0-3.16 19.49c.5.09.68-.22.68-.48v-1.7c-2.78.6-3.37-1.34-3.37-1.34-.46-1.16-1.11-1.47-1.11-1.47-.91-.62.07-.6.07-.6 1 .07 1.53 1.03 1.53 1.03.89 1.53 2.34 1.09 2.91.83.09-.65.35-1.09.63-1.34-2.22-.25-4.56-1.11-4.56-4.94 0-1.09.39-1.98 1.03-2.68-.1-.26-.45-1.27.1-2.65 0 0 .84-.27 2.75 1.02a9.56 9.56 0 0 1 5 0c1.91-1.29 2.75-1.02 2.75-1.02.55 1.38.2 2.39.1 2.65.64.7 1.03 1.59 1.03 2.68 0 3.84-2.34 4.68-4.57 4.93.36.31.68.92.68 1.85v2.75c0 .27.18.58.69.48A10 10 0 0 0 12 2z"/>,
    check:   <path d="M20 6L9 17l-5-5"/>,
    copy:    <><rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></>,
  };
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill={name === 'star' || name === 'zap' || name === 'github' ? 'currentColor' : 'none'} stroke="currentColor" strokeWidth={stroke} strokeLinecap="round" strokeLinejoin="round" {...rest}>
      {paths[name]}
    </svg>
  );
};

const Button = ({ variant = 'primary', size = 'md', children, icon, ...rest }) => {
  const base = {
    fontFamily: 'var(--font-sans)', fontWeight: 600,
    borderRadius: 10, border: '1px solid transparent',
    cursor: 'pointer', transition: 'all 200ms cubic-bezier(0.22,1,0.36,1)',
    display: 'inline-flex', alignItems: 'center', gap: 8, lineHeight: 1,
    textDecoration: 'none', whiteSpace: 'nowrap',
  };
  const sizes = {
    sm: { padding: '6px 12px', fontSize: 13, borderRadius: 8 },
    md: { padding: '10px 18px', fontSize: 14 },
    lg: { padding: '14px 22px', fontSize: 16 },
  };
  const variants = {
    primary:   { background: 'var(--accent)', color: '#fff' },
    secondary: { background: '#fff', color: 'var(--fg1)', borderColor: 'var(--border)' },
    ghost:     { background: 'transparent', color: 'var(--fg1)' },
  };
  return <button style={{ ...base, ...sizes[size], ...variants[variant] }} {...rest}>{children}{icon}</button>;
};

const Eyebrow = ({ children }) => (
  <div style={{ fontFamily: 'var(--font-sans)', fontWeight: 600, fontSize: 12, textTransform: 'uppercase', letterSpacing: '0.08em', color: 'var(--accent)' }}>{children}</div>
);

Object.assign(window, { Icon, Button, Eyebrow });
