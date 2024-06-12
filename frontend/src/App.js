// src/App.js
import React from 'react';
import { BrowserRouter as Router, Route, Routes } from 'react-router-dom';
import './App.css';
import Home from './components/Home';
import DemandChart from './components/DemandChart'; 

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/demand-chart" element={<DemandChart />} />
      </Routes>
    </Router>
  );
}

export default App;
