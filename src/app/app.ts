import { Component, signal } from '@angular/core';
import { WordCloudComponent } from './wordcloud/wordcloud';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [WordCloudComponent],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {
  protected readonly title = signal('angular-project-sample');
}
