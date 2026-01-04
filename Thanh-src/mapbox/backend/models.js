const mongoose = require('mongoose');

const WeatherSchema = new mongoose.Schema({
    timestamp: Date (ISODate),
    city: { lat: Number, lon: Number },
    temperature_2m: [Number],
    precipitation: [Number],
    cloud_cover: [Number],
    wind_speed_10m: [Number],
    wind_direction_10m_x: [Number],
    wind_direction_10m_y: [Number]
});
const collectionName = process.env.WEATHER_COLLECTION_NAME || 'stream_data';
const WeatherModel = mongoose.model('Weather', WeatherSchema, collectionName);

module.exports = { WeatherModel };