// ui_kits/marketing/Nav.jsx
const Nav = () => {
  const [scrolled, setScrolled] = React.useState(false);
  React.useEffect(() => {
    const on = () => setScrolled(window.scrollY > 20);
    window.addEventListener('scroll', on); on();
    return () => window.removeEventListener('scroll', on);
  }, []);

  const navStyle = {
    position: 'sticky', top: 0, zIndex: 50,
    height: 64,
    background: scrolled ? 'rgba(253,250,244,0.92)' : 'transparent',
    backdropFilter: scrolled ? 'blur(10px)' : 'none',
    borderBottom: scrolled ? '1px solid var(--border)' : '1px solid transparent',
    display: 'flex', alignItems: 'center',
    padding: '0 24px',
    transition: 'all 200ms',
  };
  const inner = { maxWidth: 1200, margin: '0 auto', display: 'flex', alignItems: 'center', gap: 24, width: '100%' };
  const brand = { display: 'flex', alignItems: 'center', gap: 10, fontWeight: 700, fontSize: 18, color: 'var(--fg1)', textDecoration: 'none', fontFamily: 'var(--font-sans)' };
  const links = { display: 'flex', gap: 4, marginLeft: 12, flex: 1 };
  const link = (active) => ({
    padding: '6px 10px', borderRadius: 8, fontSize: 14,
    color: active ? 'var(--accent)' : 'var(--fg2)',
    textDecoration: 'none', fontWeight: 500,
    cursor: 'pointer',
  });

  return (
    <nav style={navStyle}>
      <div style={inner}>
        <a href="#" style={brand}>
          <img src="../../assets/logo-mark.png" alt="" width={32} height={32}/>
          beava
        </a>
        <div style={links}>
          <a style={link(false)}>Docs</a>
          <a style={link(false)}>Learn</a>
          <a style={link(false)}>Blog</a>
          <a style={link(false)}>Community</a>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <a style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 13, color: 'var(--fg2)', padding: '6px 10px', border: '1px solid var(--border)', borderRadius: 8, background: '#fff', fontWeight: 500, textDecoration: 'none', fontFamily: 'var(--font-sans)' }}>
            <Icon name="github" size={14}/> 8.2k
          </a>
          <Button variant="primary" size="md" icon={<Icon name="arrow" size={14}/>}>Get started</Button>
        </div>
      </div>
    </nav>
  );
};
window.Nav = Nav;
