const mongoose = require('mongoose');

const WeatherSchema = new mongoose.Schema({
    date: String,
    location: { lat: Number, lon: Number },
    hourly_temps: [Number],
    hourly_rain: [Number],
    hourly_clouds: [Number],
    hourly_wind: [Number],
    hourly_wind_u: [Number],
    hourly_wind_v: [Number]
});

const WeatherModel = mongoose.model('Weather', WeatherSchema);

module.exports = { WeatherModel };