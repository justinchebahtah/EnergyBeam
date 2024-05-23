import React, { useState, useEffect } from 'react';

function Home() {
  const [message, setMessage] = useState('');

  useEffect(() => {
    fetch('http://localhost:5001/api')
      .then(response => {
        if (!response.ok) {
          throw new Error('Network response was not ok');
        }
        return response.json();
      })
      .then(data => setMessage(data.message))
      .catch(error => {
        console.error('There was a problem with the fetch operation:', error);
        setMessage('Error fetching data');
      });
  }, []);

  return (
    <div className="Home">
      <header className="Home-header">
        <p>{message}</p>
      </header>
    </div>
  );
}

export default Home;
