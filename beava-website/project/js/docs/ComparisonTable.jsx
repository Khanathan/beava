// js/docs/ComparisonTable.jsx
// usage:
//   <ComparisonTable
//     columns={['beava','Kafka + Flink','Materialize','Redis']}
//     usIndex={0}
//     rows={[
//       { label: 'Binaries to run',
//         cells: [
//           { v: 'yes', text: '1' },
//           { v: 'no',  text: '3+' },
//           { v: 'meh', text: '1', note: '+ external storage' },
//           { v: 'yes', text: '1' },
//         ]},
//       ...
//     ]}
//   />
const Cell = ({ v = 'yes', text, note }) => (
  <span className={'bv-v ' + v}>
    <span className="bv-v-ic"/>
    <span>
      {text}
      {note ? <span className="bv-v-note">{note}</span> : null}
    </span>
  </span>
);
const ComparisonTable = ({ columns = [], usIndex = 0, rows = [] }) => (
  <div className="bv-compare-wrap">
    <table className="bv-compare">
      <thead>
        <tr>
          <th className="row-head"></th>
          {columns.map((c, i) => (
            <th key={c} className={i === usIndex ? 'us' : ''}>{c}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((r, ri) => (
          <tr key={r.label}>
            <td className="row-label">{r.label}</td>
            {r.cells.map((c, ci) => (
              <td key={ci} className={ci === usIndex ? 'us' : ''}>
                <Cell v={c.v} text={c.text} note={c.note}/>
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);
Object.assign(window, { ComparisonTable, Cell });
