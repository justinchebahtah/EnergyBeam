import React, { useState, useEffect } from 'react';

function Test() {
  const [testMessage, setTestMessage] = useState('');

  useEffect(() => {
    fetch('http://localhost:5001/api/test')
      .then(response => {
        if (!response.ok) {
          throw new Error('Network response was not ok');
        }
        return response.json();
      })
      .then(data => setTestMessage(data.message))
      .catch(error => {
        console.error('There was a problem with the fetch operation:', error);
        setTestMessage('Error fetching data');
      });
  }, []);

  return (
    <div className="Test">
      <header className="Test-header">
        <p>{testMessage}</p>
      </header>
    </div>
  );
}

export default Test;
