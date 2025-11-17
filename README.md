# Angular 3D Word Globe Component [![Static Badge](https://img.shields.io/badge/Built_using-compo_to_repo-blue)](https://github.com/kash-pram/compo-to-repo-tool)

An interactive 3D word globe built with Angular and Three.js. Display words in a spherical arrangement that users can rotate like a globe, similar to Google Earth.

**Live Demo**: [Word Globe](https://kash-pram.github.io/angular-word-globe/)

**This is a standalone Angular component that can be easily integrated into any existing Angular project without any module configuration.**

---

## Features

✨ **3D Spherical Layout** - Words arranged on a perfect sphere using Fibonacci distribution  
🖱️ **Drag to Rotate** - Click and drag to spin the globe like Google Earth  
📱 **Touch Support** - Full touch gesture support for mobile devices  
🎯 **Always Readable** - Words automatically face the camera using billboarding  
⚡ **Smooth Momentum** - Physics-based rotation with realistic momentum  
🎨 **Customizable** - Easy to customize words, colors, sizes, and behavior  
🔄 **Auto-Rotation** - Optional continuous rotation with play/pause control  

---

## Demo

**Desktop:**
- Click and drag to rotate the word globe
- Click "Pause/Play" to toggle auto-rotation
- Click "Randomize Colors" to change word colors

**Mobile:**
- Touch and drag to rotate
- Pinch gestures supported
- Responsive design adapts to screen size

---

## What This Component Does

The Word Globe component creates an interactive 3D visualization where words float in space arranged on an invisible sphere. Users can:

1. **Rotate the Globe** - Drag with mouse or touch to spin words in any direction
2. **Read All Words** - Words always face you, no matter the rotation angle
3. **Control Animation** - Pause or play the automatic rotation
4. **Customize Colors** - Randomize word colors with a button click

Perfect for:
- Technology showcases
- Skill displays
- Tag clouds
- Product features
- Portfolio websites
- Interactive presentations

---

## Prerequisites

Before you begin, ensure you have:

- **Node.js** 16+ installed
- **npm** or **yarn** package manager
- **Angular CLI** 14+ installed globally

```bash
# Install Angular CLI globally
npm install -g @angular/cli
```

---

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/kash-pram/angular-word-globe-sample.git
cd angular-word-globe-sample
```

### 2. Install Dependencies

```bash
npm install
```

## Running the Project

### Development Server

Start the Angular development server:

```bash
ng serve
```

Or:

```bash
npm start
```

Navigate to **http://localhost:4200/** in your browser.

The application will automatically reload when you make changes to the source files.

### Build for Production

```bash
ng build
```

The build artifacts will be stored in the `dist/` directory.

### Run Tests

```bash
ng test
```

---

## Project Structure

```
angular-word-globe-sample/
├── src/
│   ├── app/
│   │   ├── word-globe/
│   │   │   ├── word-globe.component.ts    ← Main component logic
│   │   │   ├── word-globe.component.html  ← Template (minimal)
│   │   │   └── word-globe.component.css   ← Styles (empty)
│   │   └── app.component.ts
│   ├── main.ts
│   └── index.html
├── package.json
├── angular.json
└── tsconfig.json
```

---

## How to Use This Component

### Basic Usage

1. **Import the Component** in your Angular application:

```typescript
import { WordGlobeComponent } from './word-globe/word-globe.component';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [WordGlobeComponent],
  template: '<app-word-globe></app-word-globe>'
})
export class AppComponent { }
```

2. **Add to Template:**

```html
<app-word-globe></app-word-globe>
```

3. **Done!** The word globe will render with default settings.

---

## Customization Options

### 1. Change Words and Sizes

Edit the `words` array in `word-globe.component.ts`:

```typescript
private words = [
  { text: 'Angular', size: 40 },      // Larger size = bigger text
  { text: 'TypeScript', size: 35 },
  { text: 'Three.js', size: 30 },
  { text: 'Your Word', size: 28 },
  // Add more words...
];
```

**Parameters:**
- `text`: The word to display
- `size`: Relative size (20-50 recommended)

### 2. Change Globe Size

Modify the `radius` in the `createWordCloud()` method:

```typescript
const radius = 100; // Default
// const radius = 150; // Larger globe
// const radius = 50;  // Smaller, tighter globe
```

### 3. Change Font Style

Update the font in `createWordCloud()` method:

```typescript
// Current (normal weight):
context.font = `normal ${fontSize}px Arial, sans-serif`;

// Bold:
context.font = `bold ${fontSize}px Arial, sans-serif`;

// Different font:
context.font = `${fontSize}px 'Helvetica', sans-serif`;

// Italic:
context.font = `italic ${fontSize}px Arial, sans-serif`;
```

### 4. Change Colors

**Single Color:**
```typescript
// In createWordCloud() method
const colors = [
  0x000000  // Black (single color)
];
```

**Multiple Colors:**
```typescript
const colors = [
  0xff6b6b,  // Red
  0x4ecdc4,  // Teal
  0xffe66d,  // Yellow
  0xa8e6cf   // Green
];
```

**Custom Colors:**
```typescript
const colors = [
  0x1a73e8,  // Google Blue
  0xea4335,  // Google Red
  0xfbbc04,  // Google Yellow
  0x34a853   // Google Green
];
```

### 5. Adjust Rotation Speed

In the `animate()` method:

```typescript
if (this.isRotating) {
  // Auto-rotation speed
  this.rotationVelocity.x = 0.003;  // Vertical rotation
  this.rotationVelocity.y = 0.005;  // Horizontal rotation
  // Increase for faster, decrease for slower
}
```

### 6. Change Background

In the component styles:

```typescript
styles: [`
  .container {
    // Remove gradient background:
    background: transparent;
    
    // Or solid color:
    // background: #f0f0f0;
    
    // Or keep gradient (default):
    // background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  }
`]
```

### 7. Adjust Camera Distance

In the `initThree()` method:

```typescript
// For mobile
const viewportWidth = window.innerWidth;
let cameraDistance;
if (viewportWidth < 480) {
  cameraDistance = 250; // Close on phones
} else if (viewportWidth < 768) {
  cameraDistance = 300; // Medium on tablets
} else {
  cameraDistance = 400; // Normal on desktop
}
this.camera.position.z = cameraDistance;
```

### 8. Disable Features

**Disable Auto-Rotation:**
```typescript
public isRotating: boolean = false; // Change true to false
```

**Remove Buttons:**
```typescript
template: `
  <div class="container">
    <canvas #canvas></canvas>
    <!-- Remove the controls div -->
  </div>
`
```

**Disable Drag:**
Comment out the `setupMouseControls()` call in `ngOnInit()`.

---

## Advanced Customization

### Add More Words

You can add as many words as you want:

```typescript
private words = [
  { text: 'JavaScript', size: 35 },
  { text: 'Python', size: 30 },
  { text: 'Java', size: 28 },
  { text: 'React', size: 32 },
  { text: 'Vue', size: 29 },
  { text: 'Node.js', size: 31 },
  { text: 'Docker', size: 27 },
  { text: 'Kubernetes', size: 26 },
  { text: 'AWS', size: 30 },
  { text: 'MongoDB', size: 28 },
  // Add up to 50+ words
];
```

### Make Words Clickable

Add click event to words in `createWordCloud()`:

```typescript
mesh.userData = { word: word.text };

// In setupMouseControls(), add:
canvas.addEventListener('click', (e: MouseEvent) => {
  if (!this.isDragging) {
    // Add raycasting logic here to detect clicked word
    console.log('Word clicked!');
  }
});
```

### Change Word Distribution

The component uses Fibonacci sphere algorithm. To change distribution pattern, modify in `createWordCloud()`:

```typescript
// Current (Fibonacci - even distribution):
const phi = Math.acos(-1 + (2 * index) / this.words.length);
const theta = Math.sqrt(this.words.length * Math.PI) * phi;

// Alternative (random):
const phi = Math.random() * Math.PI;
const theta = Math.random() * Math.PI * 2;
```

### Responsive Text Scaling

Text already scales based on viewport. To adjust:

```typescript
// In createWordCloud()
const viewportWidth = window.innerWidth;
const scaleFactor = viewportWidth < 480 ? 1.5 : (viewportWidth < 768 ? 1.2 : 1);
const fontSize = word.size * 1 * scaleFactor;
```

---

## Component API

### Inputs

Currently no `@Input()` properties. To make configurable:

```typescript
@Input() words: { text: string; size: number }[] = [...];
@Input() radius: number = 100;
@Input() autoRotate: boolean = true;
@Input() colors: number[] = [0x000000];
```

### Outputs

Add event emitters:

```typescript
@Output() wordClicked = new EventEmitter<string>();
@Output() rotationChanged = new EventEmitter<{ x: number; y: number }>();
```

### Public Methods

- `toggleRotation()` - Toggle auto-rotation on/off
- `randomizeColors()` - Apply random colors to words

---

## Browser Support

| Browser | Version | Support |
|---------|---------|---------|
| Chrome  | Latest  | ✅ Full |
| Edge    | Latest  | ✅ Full |
| Firefox | Latest  | ✅ Full |
| Safari  | Latest  | ✅ Full |
| Mobile  | iOS 12+ | ✅ Full |
| Mobile  | Android 8+ | ✅ Full |

**Requirements:**
- WebGL support
- ES6+ JavaScript

---

## Performance Tips

### 1. Limit Number of Words

For best performance, keep words under 50:

```typescript
// Good
private words = [...]; // 15-30 words

// May impact performance
private words = [...]; // 100+ words
```

### 2. Reduce Canvas Size

Lower resolution for better performance:

```typescript
// In createWordCloud()
canvas.width = textWidth + 20; // Smaller padding
canvas.height = textHeight + 10;
```

### 3. Disable Anti-aliasing

For low-end devices:

```typescript
this.renderer = new THREE.WebGLRenderer({ 
  canvas, 
  alpha: true,
  antialias: false // Disable for performance
});
```

### 4. Optimize Update Frequency

Reduce animation frame rate if needed (not recommended unless necessary):

```typescript
// Limit to 30 FPS instead of 60 FPS
private lastFrameTime = 0;
private animate = (timestamp: number): void => {
  if (timestamp - this.lastFrameTime < 33) { // 33ms = ~30fps
    this.animationId = requestAnimationFrame(this.animate);
    return;
  }
  this.lastFrameTime = timestamp;
  // ... rest of animation code
};
```

---

## Troubleshooting

### Issue: Globe appears as an egg/oval shape

**Solution:** Use Fibonacci sphere distribution (already implemented). If still oval, check camera aspect ratio.

### Issue: Text too small on mobile

**Solution:** The component already includes responsive scaling. To increase further:

```typescript
const scaleFactor = viewportWidth < 480 ? 2.0 : (viewportWidth < 768 ? 1.5 : 1);
```

### Issue: Words not readable/facing wrong direction

**Solution:** Billboarding is already enabled. Ensure this code exists in `animate()`:

```typescript
this.wordMeshes.forEach(mesh => {
  mesh.lookAt(this.camera.position);
});
```

### Issue: Performance issues on low-end devices

**Solutions:**
1. Reduce number of words
2. Disable anti-aliasing
3. Reduce globe radius
4. Simplify animations

### Issue: Canvas not responding to drag

**Solution:** Check that `setupMouseControls()` is called in `ngOnInit()`.

---

## Technologies Used

- **Angular 20** - Frontend framework (standalone components)
- **TypeScript 5.0+** - Type-safe programming
- **Three.js r128** - 3D rendering library
- **WebGL** - Hardware-accelerated graphics

---

## License

MIT License - Free to use in personal and commercial projects.

---

## Credits

Built with ❤️ using Angular and Three.js

**Techniques Used:**
- Fibonacci Sphere Algorithm for even word distribution
- Billboarding for camera-facing text
- Momentum physics for realistic rotation

---

## FAQ

**Q: Can I use this in a commercial project?**  
A: Yes, this is MIT licensed and free to use.

**Q: How do I make the globe bigger/smaller?**  
A: Change the `radius` value in `createWordCloud()`.

**Q: Can I add images instead of text?**  
A: Yes, modify the canvas drawing code to use images instead of text.

**Q: Does this work with Angular 14/15/16?**  
A: Yes, it works with Angular 14+. Just ensure standalone components are supported.

**Q: Can I make words clickable?**  
A: Yes, implement raycasting in Three.js to detect clicks on meshes.

**Q: How do I change rotation direction?**  
A: Modify the velocity values in the `animate()` method (use negative values for opposite direction).


---

**Happy Coding! 🌐**
