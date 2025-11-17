import { ComponentFixture, TestBed } from '@angular/core/testing';

import { Wordcloud } from './wordcloud';

describe('Wordcloud', () => {
  let component: Wordcloud;
  let fixture: ComponentFixture<Wordcloud>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [Wordcloud]
    })
    .compileComponents();

    fixture = TestBed.createComponent(Wordcloud);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
