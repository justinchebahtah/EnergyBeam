import React, { useState, useEffect } from 'react';
import DemandChart from './DemandChart'; // Ensure this import is present

function Home() {
  const [prediction, setPrediction] = useState(null);
  const [message, setMessage] = useState('');

  useEffect(() => {
    fetch('http://localhost:5001/api/predict')
      .then(response => {
        if (!response.ok) {
          throw new Error('Network response was not ok');
        }
        return response.json();
      })
      .then(data => {
        setPrediction(data[0].toFixed(0)); // Round the prediction to the nearest whole number
      })
      .catch(error => {
        console.error('There was a problem with the fetch operation:', error);
        setMessage('Error fetching data');
      });
  }, []);

  return (
    <div className="p-4">
      <header className="flex items-center text-left mb-4">
        <img src="/icon.png" alt="EnergyBeam Icon" className="w-1/16 h-1/16 mr-2" />
        <h1 className="text-3xl font-bold">Welcome to EnergyBeam</h1>
      </header>
      <main>
        {message ? (
          <p className="text-red-500">{message}</p>
        ) : (
          <div className="text-lg mb-4 font-bold bg-gray-100 rounded-full px-4 py-2 inline-block">
            Next Hourly Prediction: {prediction} MW
          </div>
        )}
        <div className="bg-white p-4 rounded shadow-md mt-4">
          <DemandChart />
        </div>
      </main>
    </div>
  );
}

export default Home;
