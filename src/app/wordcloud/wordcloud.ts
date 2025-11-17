import { Component, OnInit, OnDestroy, ElementRef, ViewChild } from '@angular/core';
import * as THREE from 'three';

@Component({
  selector: 'app-word-cloud',
  standalone: true,
  templateUrl: './wordcloud.html',
  styleUrls: [`./wordcloud.css`]
})
export class WordCloudComponent implements OnInit, OnDestroy {
  @ViewChild('canvas', { static: true }) canvasRef!: ElementRef<HTMLCanvasElement>;
  
  private scene!: THREE.Scene;
  private camera!: THREE.PerspectiveCamera;
  private renderer!: THREE.WebGLRenderer;
  private wordMeshes: THREE.Mesh[] = [];
  private animationId: number = 0;
  public isRotating: boolean = true;
  
  // Mouse interaction
  private isDragging: boolean = false;
  private previousMousePosition = { x: 0, y: 0 };
  private rotation = { x: 0, y: 0 };
  private rotationVelocity = { x: 0, y: 0 };
  private sphereGroup!: THREE.Group;
  
  private words = [
    { text: 'Angular', size: 40 },
    { text: 'TypeScript', size: 35 },
    { text: 'Three.js', size: 30 },
    { text: 'Component', size: 28 },
    { text: 'Innovation', size: 32 },
    { text: 'Design', size: 25 },
    { text: 'Development', size: 30 },
    { text: 'Cloud', size: 27 },
    { text: 'Technology', size: 33 },
    { text: 'Creative', size: 26 },
    { text: 'Modern', size: 24 },
    { text: 'Digital', size: 29 },
    { text: 'Future', size: 31 },
    { text: 'Code', size: 28 },
    { text: 'Web', size: 25 },
    { text: 'AI', size: 35 },
    { text: 'Machine Learning', size: 28 },
    { text: 'Data', size: 26 },
    { text: 'Visualization', size: 27 }
  ];

  ngOnInit(): void {
    this.initThree();
    this.createWordCloud();
    this.setupMouseControls();
    this.animate();
    this.handleResize();
  }

  ngOnDestroy(): void {
    if (this.animationId) {
      cancelAnimationFrame(this.animationId);
    }
    this.renderer.dispose();
  }

  private initThree(): void {
    const canvas = this.canvasRef.nativeElement;
    
    // Scene
    this.scene = new THREE.Scene();
    
    // Create a group for the word sphere
    this.sphereGroup = new THREE.Group();
    this.scene.add(this.sphereGroup);
    
    // Camera
    this.camera = new THREE.PerspectiveCamera(
      75,
      window.innerWidth / window.innerHeight,
      0.1,
      1000
    );
    this.camera.position.z = 400;
    
    // Renderer
    this.renderer = new THREE.WebGLRenderer({ 
      canvas, 
      alpha: true,
      antialias: true 
    });
    this.renderer.setSize(window.innerWidth, window.innerHeight);
    this.renderer.setPixelRatio(window.devicePixelRatio);
  }

  private createWordCloud(): void {
    const colors = [
      0x000000  
    ];

    this.words.forEach((word, index) => {
      const canvas = document.createElement('canvas');
      const context = canvas.getContext('2d')!;
      
      // Set font first to measure text
      const fontSize = word.size * 1;
      context.font = `normal ${fontSize}px Arial, sans-serif`;
      
      // Measure the actual text width
      const metrics = context.measureText(word.text);
      const textWidth = metrics.width;
      const textHeight = fontSize * 1.0; // Add some padding
      
      // Set canvas size based on actual text dimensions with padding
      canvas.width = textWidth + 40; // Add padding
      canvas.height = textHeight + 20;
      
      // Clear and set font again (canvas resize clears it)
      context.fillStyle = 'transparent';
      context.fillRect(0, 0, canvas.width, canvas.height);
      context.font = `normal ${fontSize}px Arial, sans-serif`;
      context.fillStyle = '#ffffff';
      context.textAlign = 'center';
      context.textBaseline = 'middle';
      
      // Draw text in center
      context.fillText(word.text, canvas.width / 2, canvas.height / 2);
      
      const texture = new THREE.CanvasTexture(canvas);
      texture.needsUpdate = true;
      
      const material = new THREE.MeshBasicMaterial({
        map: texture,
        transparent: true,
        side: THREE.DoubleSide,
        color: colors[index % colors.length]
      });
      
      // Create geometry with aspect ratio matching the actual text
      const aspectRatio = canvas.width / canvas.height;
      const planeHeight = word.size * 1.0;
      const planeWidth = planeHeight * aspectRatio;
      
      const geometry = new THREE.PlaneGeometry(planeWidth, planeHeight);
      const mesh = new THREE.Mesh(geometry, material);
      
      // Position words in a sphere
      const phi = Math.acos(-1 + (2 * index) / this.words.length);
      const theta = Math.sqrt(this.words.length * Math.PI) * phi;
      const radius = 100;
      
      mesh.position.x = radius * Math.cos(theta) * Math.sin(phi);
      mesh.position.y = radius * Math.sin(theta) * Math.sin(phi);
      mesh.position.z = radius * Math.cos(phi);
      
      // Make words face outward from center (billboarding will be handled in animate)
      mesh.lookAt(0, 0, 0);
      mesh.rotateY(Math.PI);
      
      this.sphereGroup.add(mesh);
      this.wordMeshes.push(mesh);
    });
  }

  private setupMouseControls(): void {
    const canvas = this.canvasRef.nativeElement;
    
    // Mouse down
    canvas.addEventListener('mousedown', (e: MouseEvent) => {
      this.isDragging = true;
      this.previousMousePosition = { x: e.clientX, y: e.clientY };
      canvas.style.cursor = 'grabbing';
    });
    
    // Mouse move
    canvas.addEventListener('mousemove', (e: MouseEvent) => {
      if (this.isDragging) {
        const deltaX = e.clientX - this.previousMousePosition.x;
        const deltaY = e.clientY - this.previousMousePosition.y;
        
        // Update rotation based on mouse movement
        this.rotation.y += deltaX * 0.005;
        this.rotation.x += deltaY * 0.005;
        
        // Calculate velocity for momentum
        this.rotationVelocity.x = deltaY * 0.0005;
        this.rotationVelocity.y = deltaX * 0.0005;
        
        this.previousMousePosition = { x: e.clientX, y: e.clientY };
      }
    });
    
    // Mouse up
    canvas.addEventListener('mouseup', () => {
      this.isDragging = false;
      canvas.style.cursor = 'grab';
    });
    
    // Mouse leave
    canvas.addEventListener('mouseleave', () => {
      this.isDragging = false;
      canvas.style.cursor = 'grab';
    });
    
    // Touch events for mobile
    canvas.addEventListener('touchstart', (e: TouchEvent) => {
      this.isDragging = true;
      this.previousMousePosition = { 
        x: e.touches[0].clientX, 
        y: e.touches[0].clientY 
      };
    });
    
    canvas.addEventListener('touchmove', (e: TouchEvent) => {
      if (this.isDragging) {
        e.preventDefault();
        const deltaX = e.touches[0].clientX - this.previousMousePosition.x;
        const deltaY = e.touches[0].clientY - this.previousMousePosition.y;
        
        this.rotation.y += deltaX * 0.005;
        this.rotation.x += deltaY * 0.005;
        
        this.rotationVelocity.x = deltaY * 0.0005;
        this.rotationVelocity.y = deltaX * 0.0005;
        
        this.previousMousePosition = { 
          x: e.touches[0].clientX, 
          y: e.touches[0].clientY 
        };
      }
    });
    
    canvas.addEventListener('touchend', () => {
      this.isDragging = false;
    });
    
    canvas.style.cursor = 'grab';
  }

  private animate = (): void => {
    this.animationId = requestAnimationFrame(this.animate);
    
    // Apply velocity-based rotation when not dragging
    if (!this.isDragging) {
      if (this.isRotating) {
        // Auto-rotation
        this.rotationVelocity.x = 0.003;
        this.rotationVelocity.y = 0.005;
      } else {
        // Apply friction to slow down momentum
        this.rotationVelocity.x *= 0.95;
        this.rotationVelocity.y *= 0.95;
      }
      
      this.rotation.x += this.rotationVelocity.x;
      this.rotation.y += this.rotationVelocity.y;
    }
    
    // Apply rotation to the sphere group
    this.sphereGroup.rotation.x = this.rotation.x;
    this.sphereGroup.rotation.y = this.rotation.y;
    
    // Make all words face the camera (billboarding)
    this.wordMeshes.forEach(mesh => {
      const worldPos = new THREE.Vector3();
      mesh.getWorldPosition(worldPos);
      mesh.lookAt(this.camera.position);
    });
    
    this.renderer.render(this.scene, this.camera);
  };

  public toggleRotation(): void {
    this.isRotating = !this.isRotating;
  }

  public randomizeColors(): void {
    const colors = [
      0xff6b6b, 0x4ecdc4, 0xffe66d, 0xa8e6cf,
      0xff8b94, 0xc7ceea, 0xffd3b6, 0xffaaa5,
      0x95e1d3, 0xf38181, 0xaa96da, 0xfcbad3
    ];
    
    this.wordMeshes.forEach(mesh => {
      const material = mesh.material as THREE.MeshBasicMaterial;
      material.color.setHex(colors[Math.floor(Math.random() * colors.length)]);
    });
  }

  private handleResize(): void {
    window.addEventListener('resize', () => {
      this.camera.aspect = window.innerWidth / window.innerHeight;
      this.camera.updateProjectionMatrix();
      this.renderer.setSize(window.innerWidth, window.innerHeight);
    });
  }
}