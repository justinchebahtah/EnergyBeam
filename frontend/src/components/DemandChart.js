import React, { useEffect, useState } from 'react';
import { Line } from 'react-chartjs-2';
import axios from 'axios';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

const DemandChart = () => {
  const [chartData, setChartData] = useState({
    labels: [],
    datasets: [
      {
        label: 'MW Demand',
        data: [],
        borderColor: 'rgba(75,192,192,1)',
        backgroundColor: 'rgba(75,192,192,0.2)',
        fill: true,
      },
    ],
  });

  const [date, setDate] = useState('');

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await axios.get('http://127.0.0.1:5001/api/display-today');
        const data = response.data;

        const times = data.map(entry => new Date(entry.time).getHours());
        const mwValues = data.map(entry => entry.MW);

        const chartDate = new Date(data[0].time).toLocaleDateString();

        setChartData({
          labels: times,
          datasets: [
            {
              label: 'MW Demand',
              data: mwValues,
              borderColor: 'rgba(75,192,192,1)',
              backgroundColor: 'rgba(75,192,192,0.2)',
              fill: true,
            },
          ],
        });

        setDate(chartDate);
      } catch (error) {
        console.error('Error fetching the data', error);
      }
    };

    fetchData();
  }, []);

  const options = {
    plugins: {
      legend: {
        display: true,
        position: 'bottom',
      },
      title: {
        display: true,
        text: `MW Demand on ${date}`,
      },
    },
    scales: {
      x: {
        title: {
          display: true,
          text: 'Time (hours)',
        },
        ticks: {
          callback: function (value) {
            return value + ':00';
          },
        },
      },
      y: {
        title: {
          display: true,
          text: 'MW Demand',
        },
      },
    },
  };

  return (
    <div>
      <h2>MW Demand for the Latest Day</h2>
      <Line data={chartData} options={options} />
    </div>
  );
};

export default DemandChart;
