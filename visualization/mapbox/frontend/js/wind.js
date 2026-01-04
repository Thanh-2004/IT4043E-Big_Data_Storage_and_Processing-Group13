import { Config } from './config.js';

export class WindSystem {
    constructor() {
        this.linesArr = [];
    }

    initLines(stations) {
        this.linesArr = [];
        stations.forEach(station => {
            for (let i = 0; i < Config.maxLinesPerStation; i++) {
                const startPos = [
                    station.coord[0] + (Math.random() - 0.5) * Config.spawnRadius,
                    station.coord[1] + (Math.random() - 0.5) * Config.spawnRadius
                ];

                this.linesArr.push({
                    currentCoords: [startPos],
                    parentStation: station,
                    offsetX: startPos[0] - station.coord[0],
                    offsetY: startPos[1] - station.coord[1],
                    localIndex: i
                });
            }
        });
    }

    // Hàm tính toán khung hình tiếp theo
    computeNextFrame(currentZoom) {
        const features = [];
        
        // Mật độ theo Zoom
        let activeLinesCount = 50; 
        if (currentZoom > 6) activeLinesCount = 25; 
        if (currentZoom > 9) activeLinesCount = 10; 

        // Zoom Factor
        let zoomSpeedFactor = 1;
        if (currentZoom < 6) zoomSpeedFactor = 1.5; 

        // Độ dài mục tiêu (đốt)
        let targetLengthKm = Config.targetVisualLengthKm;
        if (currentZoom > 7) targetLengthKm = 8;
        if (currentZoom > 9) targetLengthKm = 3;

        for (let line of this.linesArr) {
            if (line.localIndex >= activeLinesCount) continue;

            const lastCoord = line.currentCoords[line.currentCoords.length - 1];
            const station = line.parentStation;

            // Di chuyển
            const moveDist = (station.speed * Config.lineSpeed * zoomSpeedFactor) + 0.02;
            const dest = turf.destination(turf.point(lastCoord), moveDist, station.bearing, {units: 'kilometers'});
            const nextCoord = dest.geometry.coordinates;
            
            line.currentCoords.push(nextCoord);

            // Check giới hạn vùng bay
            const distFromCenter = turf.distance(turf.point(nextCoord), turf.point(station.coord), {units: 'kilometers'});
            if (distFromCenter > Config.maxRadiusKm) {
                const respawnPos = [
                    station.coord[0] + line.offsetX,
                    station.coord[1] + line.offsetY
                ];
                line.currentCoords = [respawnPos];
            }

            // Cắt đuôi (Logic độ dài cố định)
            const allowedSegments = Math.floor(targetLengthKm / moveDist);
            const finalSegments = Math.max(2, allowedSegments);

            if (line.currentCoords.length > finalSegments) { 
                line.currentCoords.shift();
                while(line.currentCoords.length > finalSegments) {
                        line.currentCoords.shift();
                }
            }

            if (line.currentCoords.length > 1) {
                features.push(turf.lineString(line.currentCoords));
            }
        }

        return turf.featureCollection(features);
    }
}