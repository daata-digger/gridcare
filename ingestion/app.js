import React, { useEffect, useState } from 'react';
import socketIOClient from 'socket.io-client';

const App = () => {
  const [weatherData, setWeatherData] = useState({});
  const SOCKET_SERVER_URL = "http://localhost:5000";  // Point to your Flask server

  useEffect(() => {
    const socket = socketIOClient(SOCKET_SERVER_URL);
    socket.on("weather_update", (data) => {
      setWeatherData(data);
    });

    return () => socket.disconnect();
  }, []);

  return (
    <div>
      <h1>Real-Time Weather Data</h1>
      <div>
        <strong>ISO Code:</strong> {weatherData.iso_code}<br />
        <strong>Temperature:</strong> {weatherData.temperature} °C<br />
        <strong>Humidity:</strong> {weatherData.humidity} %<br />
        <strong>Wind Speed:</strong> {weatherData.wind_speed} m/s<br />
      </div>
    </div>
  );
};

export default App;
