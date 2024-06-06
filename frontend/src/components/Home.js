import React, { useState, useEffect } from 'react';

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
    <div className="Home">
      <header className="Home-header">
        {message ? (
          <p>{message}</p>
        ) : (
          <p>Next Hourly Prediction: {prediction} MW</p>
        )}
      </header>
    </div>
  );
}

export default Home;
