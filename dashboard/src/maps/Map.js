import React, { useEffect, useState } from 'react';
import ReactMapGL, { Marker } from 'react-map-gl';
import axios from 'axios';

const Map = () => {
  const [viewport, setViewport] = useState({
    latitude: 37.8,
    longitude: -122.4,
    zoom: 4
  });
  const [priceData, setPriceData] = useState([]);

  useEffect(() => {
    // Fetch price data from the API (replace with actual endpoint)
    axios.get('/api/prices')  // Replace with actual API endpoint for nodal prices
      .then(response => {
        setPriceData(response.data);
      })
      .catch(error => console.log(error));
  }, []);

  return (
    <ReactMapGL
      {...viewport}
      width="100%"
      height="600px"
      mapboxApiAccessToken="your-mapbox-token"  // Add your Mapbox token here
      onViewportChange={(nextViewport) => setViewport(nextViewport)}
    >
      {priceData.map((price, index) => (
        <Marker key={index} latitude={price.lat} longitude={price.lon}>
          <div>
            <p>{price.iso}</p>
            <p>{price.price}</p>
          </div>
        </Marker>
      ))}
    </ReactMapGL>
  );
};

export default Map;
