import React, { useEffect, useState } from 'react';
import { Line } from 'react-chartjs-2';
import axios from 'axios';

const TimeSeriesChart = () => {
  const [data, setData] = useState({
    labels: [],
    datasets: [{
      label: 'Grid Load MW',
      data: [],
      fill: false,
      borderColor: 'blue',
      tension: 0.1
    }]
  });

  useEffect(() => {
    // Fetch time series data (replace with your API endpoint)
    axios.get('/api/grid/load')  // Replace with actual API endpoint
      .then(response => {
        const gridData = response.data;
        const labels = gridData.map(item => item.timestamp);
        const dataset = gridData.map(item => item.load_mw);
        
        setData({
          labels,
          datasets: [{
            ...data.datasets[0],
            data: dataset
          }]
        });
      })
      .catch(error => console.log(error));
  }, []);

  return (
    <div>
      <h2>Grid Load Over Time</h2>
      <Line data={data} />
    </div>
  );
};

export default TimeSeriesChart;
