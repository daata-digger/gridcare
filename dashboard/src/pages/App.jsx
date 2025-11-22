import React, { useState, useEffect } from 'react';

const App = () => {
  const [gridData, setGridData] = useState(null);
  const [prices, setPrices] = useState(null);

  useEffect(() => {
    // Fetch grid data
    fetch("http://127.0.0.1:8000/api/grid/load")
      .then(response => response.json())
      .then(data => setGridData(data));

    // Fetch prices data
    fetch("http://127.0.0.1:8000/api/prices")
      .then(response => response.json())
      .then(data => setPrices(data));
  }, []);

  return (
    <div>
      <h1>Welcome to GridCARE Dashboard</h1>
      <div>
        <h2>Energy Grid Information</h2>
        {gridData ? (
          <pre>{JSON.stringify(gridData, null, 2)}</pre>
        ) : (
          <p>Loading data...</p>
        )}
      </div>
      <div>
        <h2>Prices</h2>
        {prices ? (
          <pre>{JSON.stringify(prices, null, 2)}</pre>
        ) : (
          <p>Loading prices...</p>
        )}
      </div>
    </div>
  );
};

export default App;
