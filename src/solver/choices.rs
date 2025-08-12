#[derive(Clone)]
pub struct MaskRange {
    start: usize,
    end: usize,
    current: usize
}

impl MaskRange {
    pub fn ended(&self) -> bool {
        self.current == self.end
    }

    pub fn advance(&mut self) -> bool {
        if self.current < self.end {
            self.current += 1;
        }

        self.current < self.end
    }

    pub fn reset(&mut self) {
        self.current = self.start
    }

    pub fn get(&self) -> usize {
        self.current
    }
}

#[derive(Clone)]
pub struct AllRange {
    start: usize,
    end: usize,
    current: usize
}

impl AllRange {
    pub fn ended(&self) -> bool {
        self.current == self.end
    }

    pub fn advance(&mut self) -> bool {
        if self.current < self.end {
            self.current += 1;
        }

        self.current < self.end
    }

    pub fn get_start(&self) -> usize {
        self.start
    }

    pub fn get_end(&self) -> usize {
        self.end
    }

    pub fn reset(&mut self) {
        self.current = self.start
    }

    pub fn get(&self) -> usize {
        self.current
    }
}


enum ChoiceGeneratorEntry {
    MaskRange(MaskRange),
    AllRange(AllRange)
}

impl ChoiceGeneratorEntry {
    /*
    pub fn ended(&self) -> bool {
        match self {
            ChoiceGeneratorEntry::MaskRange(mask_range) => mask_range.ended(),
            ChoiceGeneratorEntry::AllRange(_) => true,
        }
    }
    */

    pub fn advance(&mut self) -> bool {
        match self {
            ChoiceGeneratorEntry::MaskRange(mask_range) => mask_range.advance(),
            ChoiceGeneratorEntry::AllRange(_) => false,
        }
    }

    pub fn reset(&mut self) {
        match self {
            ChoiceGeneratorEntry::MaskRange(mask_range) => mask_range.reset(),
            ChoiceGeneratorEntry::AllRange(_) => {},
        }
    }
}

pub struct ChoiceGenerator  {
    index: usize,
    ended: bool,
    selection: Vec<ChoiceGeneratorEntry>
}

impl ChoiceGenerator {
    pub fn empty() -> ChoiceGenerator {
        ChoiceGenerator {
            index: 0,
            ended: true,
            selection: Vec::new()
        }
    }

    pub fn ended(&self) -> bool {
        self.ended
    }

    pub fn reset_from(&mut self, choices: &[usize], target: usize) {
        self.selection.clear();

        if choices.len() == 0 || choices.iter().any(|value| *value == 0) {
            self.index = 0;
            self.ended = true;
            return;
        }

        let mut best: Vec<usize> = choices.to_vec();
        let mut best_count: usize = Self::combinations(&best);
        let mut staging: Vec<usize> = vec![1; choices.len()];

        self.reset_from_internal(&mut staging, &mut best, &mut best_count, &choices, 0, target);

        self.ended = false;
        self.index = choices.len() - 1;

        let total_count = Self::combinations(choices);
        // In the future, if we want the task iterator to only generator all items
        // when a best fit cannot be achieved, then we should skip the best and total 
        // count check.
        for index in 0..best.len() {
            if best[index] == choices[index] && total_count != best_count {
                let mask_range = MaskRange {
                    start: 0,
                    end: choices[index],
                    current: 0
                };

                self.selection.push(ChoiceGeneratorEntry::MaskRange(mask_range));
            } else {
                let all_range = AllRange {
                    start: 0,
                    end: choices[index],
                    current: 0
                };

                self.selection.push(ChoiceGeneratorEntry::AllRange(all_range));
            }
        }
        
    }

    pub fn reset_from_internal(
        &mut self, 
        staging: &mut [usize],
        best: &mut [usize], 
        best_count: &mut usize,
        choices: &[usize], 
        depth: usize,
        target: usize
    ) {
        if depth == choices.len() {
            let staging_count = Self::combinations(&staging);

            if staging_count >= target && staging_count < *best_count {
                for index in 0..best.len() {
                    best[index] = staging[index];
                }
                *best_count = staging_count;
            }

            return;
        }

        staging[depth] = choices[depth];
        let staging_count = Self::combinations(&staging);
        if staging_count < *best_count {
            self.reset_from_internal(staging, best, best_count, choices, depth + 1, target);
        }

        if choices[depth] != 1 {
            staging[depth] = 1;
            let staging_count = Self::combinations(&staging);
            if staging_count < *best_count {
                self.reset_from_internal(staging, best, best_count, choices, depth + 1, target);
            }
        }

    }

    fn combinations(choices: &[usize]) -> usize {
        let mut calculated: usize = choices[0];
        for index in 1..choices.len() {

            let choice = choices[index];
            calculated = calculated
                .checked_mul(choice)
                .unwrap_or_else(|| usize::MAX);
    
            if calculated == usize::MAX || calculated == 0 {
                return calculated;
            }
        }

        calculated
    }

    pub fn next(&mut self) -> bool {
        if self.ended {
            return false;
        }

        let mut next_index = self.index;
        let mut next_advance = self.index != 0;

        if !self.selection[self.index].advance() {
            loop {
                if next_index == 0 && !next_advance {
                    self.ended = true;
                    return false;
                }

                next_index = next_index - 1;
                next_advance = self.selection[next_index].advance();

                if next_advance {
                    for reset_index in (next_index + 1)..self.selection.len() {
                        self.selection[reset_index].reset();
                    }
                    self.index = self.selection.len() - 1;
                    return true;
                }
            }
        } else {
            for reset_index in (next_index + 1)..self.selection.len() {
                self.selection[reset_index].reset();
            }
            self.index = self.selection.len() - 1;
            return true;
        }
    }

    pub fn get(&self, consumer: &mut ChoiceConsumer) {
        consumer.selection.clear();
        for value in self.selection.iter() {
            let res = match value {
                ChoiceGeneratorEntry::MaskRange(mask_range) => ChoiceConsumerEntry::Mask(mask_range.get()),
                ChoiceGeneratorEntry::AllRange(all_range) => ChoiceConsumerEntry::AllRange(all_range.clone()),
            };

            consumer.selection.push(res);
        }
        consumer.index = std::cmp::max(1, consumer.selection.len()) - 1;
        consumer.ended = consumer.selection.len() == 0;
    }
}

pub enum ChoiceConsumerEntry {
    Mask(usize),
    AllRange(AllRange),
}

impl ChoiceConsumerEntry {
    pub fn ended(&self) -> bool {
        match self {
            ChoiceConsumerEntry::AllRange(all_range) => all_range.ended(),
            ChoiceConsumerEntry::Mask(_) => true,
        }
    }

    pub fn advance(&mut self) -> bool {
        match self {
            ChoiceConsumerEntry::Mask(_) => false,
            ChoiceConsumerEntry::AllRange(all_range) => all_range.advance(),
        }
    }

    pub fn reset(&mut self) {
        match self {
            ChoiceConsumerEntry::Mask(_) => {},
            ChoiceConsumerEntry::AllRange(all_range) => all_range.reset(),
        }
    }

    pub fn get(&self) -> usize {
        match self {
            ChoiceConsumerEntry::Mask(mask) => { *mask },
            ChoiceConsumerEntry::AllRange(all_range) => all_range.get(),
        }
    }
}

pub struct ChoiceConsumer {
    selection: Vec<ChoiceConsumerEntry>,
    index: usize,
    ended: bool
}

impl ChoiceConsumer {
    pub fn empty() -> ChoiceConsumer {
        ChoiceConsumer {
            index: 0,
            ended: true,
            selection: Vec::new()        
        }
    }

    pub fn ended(&self) -> bool {
        self.ended
    }

    pub fn next(&mut self) -> bool {
        if self.ended {
            return false;
        }

        let mut next_index = self.index;
        let mut next_advance = self.index != 0;

        if !self.selection[self.index].advance() {
            loop {
                if next_index == 0 && !next_advance {
                    self.ended = true;
                    return false;
                }

                next_index = next_index - 1;
                next_advance = self.selection[next_index].advance();

                if next_advance {
                    for reset_index in (next_index + 1)..self.selection.len() {
                        self.selection[reset_index].reset();
                    }
                    self.index = self.selection.len() - 1;
                    return true;
                }
            }
        } else {
            for reset_index in (next_index + 1)..self.selection.len() {
                self.selection[reset_index].reset();
            }
            self.index = self.selection.len() - 1;
            return true;
        }
    }

    pub fn get(&self, index: usize) -> &ChoiceConsumerEntry {
        &self.selection[index]
    }

    pub fn len(&self) -> usize {
        self.selection.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn exact_combinations() {
        let choices: &[usize] = &[2, 2, 4, 5];
        let mut generator = ChoiceGenerator::empty();
        let mut consumer = ChoiceConsumer::empty();

        generator.reset_from(choices, 5);

        let mut actual_count: usize = 0;
        let mut generator_count = 0;

        while !generator.ended() {
            generator.get(&mut consumer);
            generator_count += 1;

            let mut total = 1;
            for index in 0..consumer.len() {
                let entry = consumer.get(index);
                let size = match entry {
                    ChoiceConsumerEntry::Mask(_) => 1,
                    ChoiceConsumerEntry::AllRange(all_range) => all_range.get_end(),
                };
                total *= size;

            }
            actual_count += total;

            generator.next();
        }

        assert_eq!(5, generator_count);
        assert_eq!(80, actual_count);
    }

    #[test]
    pub fn odd_combinations_cant_fit() {
        let choices: &[usize] = &[2, 2, 4, 5];
        let mut generator = ChoiceGenerator::empty();

        generator.reset_from(choices, 80);

        let mut actual_count: usize = 0;

        while !generator.ended() {
            actual_count += 1;
            generator.next();
        }

        assert_eq!(1, actual_count);
    }

    #[test]
    pub fn odd_combinations() {
        let choices: &[usize] = &[2, 2, 4, 5];
        let mut generator = ChoiceGenerator::empty();
        let mut consumer = ChoiceConsumer::empty();

        generator.reset_from(choices, 3);

        let mut actual_count: usize = 0;
        let mut generator_count = 0;

        while !generator.ended() {
            generator.get(&mut consumer);
            generator_count += 1;

            let mut total = 1;
            println!("");

            for index in 0..consumer.len() {
                let entry = consumer.get(index);
                let size = match entry {
                    ChoiceConsumerEntry::Mask(mask) => {
                        print!("[{}] ", mask);
                        1
                    },
                    ChoiceConsumerEntry::AllRange(all_range) => {
                        print!("[{}..{}] ", all_range.get_start(), all_range.get_end());
                        all_range.get_end()
                    },
                };
                total *= size;

            }
            actual_count += total;

            generator.next();
        }

        assert_eq!(4, generator_count);
        assert_eq!(80, actual_count);
    }

    #[test]
    pub fn even_combinations() {
        let choices: &[usize] = &[2, 2, 4, 5];
        let mut generator = ChoiceGenerator::empty();
        let mut result = ChoiceConsumer::empty();

        generator.reset_from(choices, 4);

        let mut actual_count: usize = 0;

        while !generator.ended() {
            actual_count += 1;
            generator.get(&mut result);
            generator.next();
        }

        assert_eq!(4, actual_count);
    }

    #[test]
    pub fn empty_combinations() {
        let choices: &[usize] = &[];
        let mut generator = ChoiceGenerator::empty();
        let mut consumer = ChoiceConsumer::empty();

        generator.reset_from(choices, 2);

        let mut actual_count: usize = 0;
        let mut generator_count = 0;

        while !generator.ended() {
            generator.get(&mut consumer);
            generator_count += 1;

            let mut total = 1;
            for index in 0..consumer.len() {
                let entry = consumer.get(index);
                let size = match entry {
                    ChoiceConsumerEntry::Mask(_) => 1,
                    ChoiceConsumerEntry::AllRange(all_range) => all_range.get_end(),
                };
                total *= size;

            }
            actual_count += total;

            generator.next();
        }

        assert_eq!(0, generator_count);
        assert_eq!(0, actual_count);
    }

    #[test]
    pub fn one_combination() {
        let choices: &[usize] = &[1];
        let mut generator = ChoiceGenerator::empty();
        let mut consumer = ChoiceConsumer::empty();

        generator.reset_from(choices, 1);

        let mut actual_count: usize = 0;
        let mut generator_count = 0;

        while !generator.ended() {
            generator.get(&mut consumer);
            generator_count += 1;

            let mut total = 1;
            for index in 0..consumer.len() {
                let entry = consumer.get(index);
                let size = match entry {
                    ChoiceConsumerEntry::Mask(_) => 1,
                    ChoiceConsumerEntry::AllRange(all_range) => all_range.get_end(),
                };
                total *= size;

            }
            actual_count += total;

            generator.next();
        }

        assert_eq!(1, generator_count);
        assert_eq!(1, actual_count);
    }
}